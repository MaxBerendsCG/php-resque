<?php
/**
 * Base Resque class.
 *
 * @author		Chris Boulton <chris@bigcommerce.com>
 * @license		http://www.opensource.org/licenses/mit-license.php
 */
class Resque
{
    const VERSION = '1.2';

    const DEFAULT_INTERVAL = 5;

    /**
     * @var Resque_Redis Instance of Resque_Redis that talks to redis.
     */
    public static $redis = null;

    /**
     * @var mixed Host/port conbination separated by a colon, or a nested
     *            array of server swith host/port pairs
     */
    protected static $redisServer = null;

    /**
     * @var int ID of Redis database to select.
     */
    protected static $redisDatabase = 0;

    /**
     * Given a host/port combination separated by a colon, set it as
     * the redis server that Resque will talk to.
     *
     * @param mixed $server   Host/port combination separated by a colon, DSN-formatted URI, or
     *                        a callable that receives the configured database ID
     *                        and returns a Resque_Redis instance, or
     *                        a nested array of servers with host/port pairs.
     * @param int   $database
     */
    public static function setBackend($server, $database = 0)
    {
        self::$redisServer = $server;
        self::$redisDatabase = $database;
        self::$redis = null;
    }

    /**
     * Return an instance of the Resque_Redis class instantiated for Resque.
     *
     * @return Resque_Redis Instance of Resque_Redis.
     */
    public static function redis()
    {
        if (self::$redis !== null) {
            return self::$redis;
        }

        if (is_callable(self::$redisServer)) {
            self::$redis = call_user_func(self::$redisServer, self::$redisDatabase);
        } else {
            self::$redis = new Resque_Redis(self::$redisServer, self::$redisDatabase);
        }

        return self::$redis;
    }

    /**
     * fork() helper method for php-resque that handles issues PHP socket
     * and phpredis have with passing around sockets between child/parent
     * processes.
     *
     * Will close connection to Redis before forking.
     *
     * @return int Return vars as per pcntl_fork()
     */
    public static function fork()
    {
        if (!function_exists('pcntl_fork')) {
            return -1;
        }

        // Close the connection to Redis before forking.
        // This is a workaround for issues phpredis has.
        self::$redis = null;

        $pid = pcntl_fork();
        if ($pid === -1) {
            throw new RuntimeException('Unable to fork child worker.');
        }

        return $pid;
    }

    /**
     * Push a job to the end of a specific queue. If the queue does not
     * exist, then create it as well.
     *
     * @param string $queue The name of the queue to add the job to.
     * @param array  $item  Job description as an array to be JSON encoded.
     */
    public static function push($queue, $item)
    {
        self::redis()->sadd('queues', $queue);
        $length = self::redis()->rpush('queue:'.$queue, json_encode($item));
        if ($length < 1) {
            return false;
        }

        return true;
    }

    /**
     * Directly append an item to the delayed queue schedule.
     *
     * @param DateTime|int $timestamp Timestamp job is scheduled to be run at.
     * @param array        $item      Hash of item to be pushed to schedule.
     */
    public static function delayedPush($timestamp, $item)
    {
        $timestamp = self::getTimestamp($timestamp);
        $redis = self::redis();
        $redis->rpush('delayed:'.$timestamp, json_encode($item));

        $redis->zadd('delayed_queue_schedule', $timestamp, $timestamp);
    }

    /**
     * Pop an item off the end of the specified queue, decode it and
     * return it.
     *
     * @param string $queue The name of the queue to fetch an item from.
     *
     * @return array Decoded item from the queue.
     */
    public static function pop($queue)
    {
        $item = self::redis()->lpop('queue:'.$queue);

        if (!$item) {
            return;
        }

        return json_decode($item, true);
    }

    /**
     * Remove items of the specified queue.
     *
     * @param string $queue The name of the queue to fetch an item from.
     * @param array  $items
     *
     * @return int number of deleted items
     */
    public static function dequeue($queue, $items = array())
    {
        if (count($items) > 0) {
            return self::removeItems($queue, $items);
        } else {
            return self::removeList($queue);
        }
    }

    /**
     * Remove a delayed job from the queue.
     *
     * note: you must specify exactly the same
     * queue, class and arguments that you used when you added
     * to the delayed queue
     *
     * also, this is an expensive operation because all delayed keys have tobe
     * searched
     *
     * @param $queue
     * @param $class
     * @param $args
     *
     * @return int number of jobs that were removed
     */
    public static function removeDelayed($queue, $class, $args)
    {
        $destroyed = 0;
        $item = json_encode(self::jobToHash($queue, $class, $args));
        $redis = self::redis();

        foreach ($redis->keys('delayed:*') as $key) {
            $key = $redis->removePrefix($key);
            $destroyed += $redis->lrem($key, 0, $item);
        }

        return $destroyed;
    }

    /**
     * removed a delayed job queued for a specific timestamp.
     *
     * note: you must specify exactly the same
     * queue, class and arguments that you used when you added
     * to the delayed queue
     *
     * @param $timestamp
     * @param $queue
     * @param $class
     * @param $args
     *
     * @return mixed
     */
    public static function removeDelayedJobFromTimestamp($timestamp, $queue, $class, $args)
    {
        $key = 'delayed:'.self::getTimestamp($timestamp);
        $item = json_encode(self::jobToHash($queue, $class, $args));
        $redis = self::redis();
        $count = $redis->lrem($key, 0, $item);
        self::cleanupTimestamp($key, $timestamp);

        return $count;
    }

    /**
     * Remove specified queue.
     *
     * @param string $queue The name of the queue to remove.
     *
     * @return int Number of deleted items
     */
    public static function removeQueue($queue)
    {
        $num = self::removeList($queue);
        self::redis()->srem('queues', $queue);

        return $num;
    }

    /**
     * Pop an item off the end of the specified queues, using blocking list pop,
     * decode it and return it.
     *
     * @param array $queues
     * @param int   $timeout
     *
     * @return null|array Decoded item from the queue.
     */
    public static function blpop(array $queues, $timeout)
    {
        $list = array();
        foreach ($queues as $queue) {
            $list[] = 'queue:'.$queue;
        }

        $item = self::redis()->blpop($list, (int) $timeout);

        if (!$item) {
            return;
        }

        /*
         * Normally the Resque_Redis class returns queue names without the prefix
         * But the blpop is a bit different. It returns the name as prefix:queue:name
         * So we need to strip off the prefix:queue: part
         */
        $queue = substr($item[0], strlen(self::redis()->getPrefix().'queue:'));

        return array(
        'queue' => $queue,
        'payload' => json_decode($item[1], true),
        );
    }

    /**
     * Return the size (number of pending jobs) of the specified queue.
     *
     * @param string $queue name of the queue to be checked for pending jobs
     *
     * @return int The size of the queue.
     */
    public static function size($queue)
    {
        return self::redis()->llen('queue:'.$queue);
    }

    /**
     * Get the total number of jobs in the delayed schedule.
     *
     * @return int Number of scheduled jobs.
     */
    public static function getDelayedQueueScheduleSize()
    {
        return (int) self::redis()->zcard('delayed_queue_schedule');
    }

    /**
     * Create a new job and save it to the specified queue.
     *
     * @param string $queue       The name of the queue to place the job in.
     * @param string $class       The name of the class that contains the code to execute the job.
     * @param array  $args        Any optional arguments that should be passed when the job is executed.
     * @param bool   $trackStatus Set to true to be able to monitor the status of a job.
     *
     * @return string|bool Job ID when the job was created, false if creation was cancelled due to beforeEnqueue
     */
    public static function enqueue($queue, $class, $args = null, $trackStatus = false)
    {
        $id = self::generateJobId();
        $hookParams = array(
            'class' => $class,
            'args' => $args,
            'queue' => $queue,
            'id' => $id,
        );
        try {
            Resque_Event::trigger('beforeEnqueue', $hookParams);
        } catch (Resque_Job_DontCreate $e) {
            return false;
        }

        Resque_Job::create($queue, $class, $args, $trackStatus, $id);
        Resque_Event::trigger('afterEnqueue', $hookParams);

        return $id;
    }

    /**
     * Enqueue a job in a given number of seconds from now.
     *
     * Identical to Resque::enqueue, however the first argument is the number
     * of seconds before the job should be executed.
     *
     * @param int    $in    Number of seconds from now when the job should be executed.
     * @param string $queue The name of the queue to place the job in.
     * @param string $class The name of the class that contains the code to execute the job.
     * @param array  $args  Any optional arguments that should be passed when the job is executed.
     */
    public static function enqueueIn($in, $queue, $class, array $args = array())
    {
        self::enqueueAt(time() + $in, $queue, $class, $args);
    }

    /**
     * Enqueue a job for execution at a given timestamp.
     *
     * Identical to Resque::enqueue, however the first argument is a timestamp
     * (either UNIX timestamp in integer format or an instance of the DateTime
     * class in PHP).
     *
     * @param DateTime|int $at    Instance of PHP DateTime object or int of UNIX timestamp.
     * @param string       $queue The name of the queue to place the job in.
     * @param string       $class The name of the class that contains the code to execute the job.
     * @param array        $args  Any optional arguments that should be passed when the job is executed.
     */
    public static function enqueueAt($at, $queue, $class, $args = array())
    {
        //self::validateJob($class, $queue);

        $job = self::jobToHash($queue, $class, $args);
        self::delayedPush($at, $job);

        Resque_Event::trigger('afterSchedule', array(
            'at' => $at,
            'queue' => $queue,
            'class' => $class,
            'args' => $args,
        ));
    }

    /**
     * Reserve and return the next available job in the specified queue.
     *
     * @param string $queue Queue to fetch next available job from.
     *
     * @return Resque_Job Instance of Resque_Job to be processed, false if none or error.
     */
    public static function reserve($queue)
    {
        return Resque_Job::reserve($queue);
    }

    /**
     * Get an array of all known queues.
     *
     * @return array Array of queues.
     */
    public static function queues()
    {
        $queues = self::redis()->smembers('queues');
        if (!is_array($queues)) {
            $queues = array();
        }

        return $queues;
    }

    /**
     * Remove Items from the queue
     * Safely moving each item to a temporary queue before processing it
     * If the Job matches, counts otherwise puts it in a requeue_queue
     * which at the end eventually be copied back into the original queue.
     *
     * @private
     *
     * @param string $queue The name of the queue
     * @param array  $items
     *
     * @return int number of deleted items
     */
    private static function removeItems($queue, $items = array())
    {
        $counter = 0;
        $originalQueue = 'queue:'.$queue;
        $tempQueue = $originalQueue.':temp:'.time();
        $requeueQueue = $tempQueue.':requeue';

        // move each item from original queue to temp queue and process it
        $finished = false;
        while (!$finished) {
            $string = self::redis()->rpoplpush($originalQueue, self::redis()->getPrefix().$tempQueue);

            if (!empty($string)) {
                if (self::matchItem($string, $items)) {
                    self::redis()->rpop($tempQueue);
                    ++$counter;
                } else {
                    self::redis()->rpoplpush($tempQueue, self::redis()->getPrefix().$requeueQueue);
                }
            } else {
                $finished = true;
            }
        }

        // move back from temp queue to original queue
        $finished = false;
        while (!$finished) {
            $string = self::redis()->rpoplpush($requeueQueue, self::redis()->getPrefix().$originalQueue);
            if (empty($string)) {
                $finished = true;
            }
        }

        // remove temp queue and requeue queue
        self::redis()->del($requeueQueue);
        self::redis()->del($tempQueue);

        return $counter;
    }

    /**
     * matching item
     * item can be ['class'] or ['class' => 'id'] or ['class' => {:foo => 1, :bar => 2}].
     *
     * @private
     *
     * @params string $string redis result in json
     * @params $items
     *
     * @return (bool)
     */
    private static function matchItem($string, $items)
    {
        $decoded = json_decode($string, true);

        foreach ($items as $key => $val) {
            # class name only  ex: item[0] = ['class']
        if (is_numeric($key)) {
            if ($decoded['class'] == $val) {
                return true;
            }
        # class name with args , example: item[0] = ['class' => {'foo' => 1, 'bar' => 2}]
        } elseif (is_array($val)) {
            $decodedArgs = (array) $decoded['args'][0];
            if ($decoded['class'] == $key &&
            count($decodedArgs) > 0 && count(array_diff($decodedArgs, $val)) == 0) {
                return true;
            }
        # class name with ID, example: item[0] = ['class' => 'id']
        } else {
            if ($decoded['class'] == $key && $decoded['id'] == $val) {
                return true;
            }
        }
        }

        return false;
    }

    /**
     * Remove List.
     *
     * @private
     *
     * @params string $queue the name of the queue
     *
     * @return int number of deleted items belongs to this list
     */
    private static function removeList($queue)
    {
        $counter = self::size($queue);
        $result = self::redis()->del('queue:'.$queue);

        return ($result == 1) ? $counter : 0;
    }

    /**
     * If there are no jobs for a given key/timestamp, delete references to it.
     *
     * Used internally to remove empty delayed: items in Redis when there are
     * no more jobs left to run at that timestamp.
     *
     * @param string $key       Key to count number of items at.
     * @param int    $timestamp Matching timestamp for $key.
     */
    private static function cleanupTimestamp($key, $timestamp)
    {
        $timestamp = self::getTimestamp($timestamp);
        $redis = self::redis();

        if ($redis->llen($key) == 0) {
            $redis->del($key);
            $redis->zrem('delayed_queue_schedule', $timestamp);
        }
    }

    /**
     * Convert a timestamp in some format in to a unix timestamp as an integer.
     *
     * @param DateTime|int $timestamp Instance of DateTime or UNIX timestamp.
     *
     * @return int Timestamp
     *
     * @throws ResqueScheduler_InvalidTimestampException
     */
    private static function getTimestamp($timestamp)
    {
        if ($timestamp instanceof DateTime) {
            $timestamp = $timestamp->getTimestamp();
        }

        if ((int) $timestamp != $timestamp) {
            throw new Resque_Exception(
                'The supplied timestamp value could not be converted to an integer.'
            );
        }

        return (int) $timestamp;
    }

    /**
     * Find the first timestamp in the delayed schedule before/including the timestamp.
     *
     * Will find and return the first timestamp upto and including the given
     * timestamp. This is the heart of the ResqueScheduler that will make sure
     * that any jobs scheduled for the past when the worker wasn't running are
     * also queued up.
     *
     * @param DateTime|int $timestamp Instance of DateTime or UNIX timestamp.
     *                                Defaults to now.
     *
     * @return int|false UNIX timestamp, or false if nothing to run.
     */
    public static function nextDelayedTimestamp($at = null)
    {
        if ($at === null) {
            $at = time();
        } else {
            $at = self::getTimestamp($at);
        }

        $items = self::redis()->zrangebyscore('delayed_queue_schedule', '-inf', $at, array('limit' => array(0, 1)));
        if (!empty($items)) {
            return $items[0];
        }

        return false;
    }

    /**
     * Pop a job off the delayed queue for a given timestamp.
     *
     * @param DateTime|int $timestamp Instance of DateTime or UNIX timestamp.
     *
     * @return array Matching job at timestamp.
     */
    public static function nextItemForTimestamp($timestamp)
    {
        $timestamp = self::getTimestamp($timestamp);
        $key = 'delayed:'.$timestamp;

        $item = json_decode(self::redis()->lpop($key), true);

        self::cleanupTimestamp($key, $timestamp);

        return $item;
    }

    /**
     * Generate hash of all job properties to be saved in the scheduled queue.
     *
     * @param string $queue Name of the queue the job will be placed on.
     * @param string $class Name of the job class.
     * @param array  $args  Array of job arguments.
     */
    private static function jobToHash($queue, $class, $args)
    {
        return array(
            'class' => $class,
            'args' => array($args),
            'queue' => $queue,
        );
    }

    /*
     * Generate an identifier to attach to a job for status tracking.
     *
     * @return string
     */
    public static function generateJobId()
    {
        return md5(uniqid('', true));
    }
}

package com.hyk.distributed_lock.demo;

import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

@RestController
public class IndexController {

    @Autowired
    private Redisson redisson;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @RequestMapping("/deduct_stock")
    public String deductStock() throws Exception {
        version1();
        version2();
        return "end";
    }

    /**
     * redis 中库存有50个,减库存逻辑
     * 最初版本，多线程情况下，会有线程并发问题：
     */
    private void version1() throws Exception {
        int stock = Integer.parseInt(stringRedisTemplate.opsForValue().get("stock"));
        if (stock > 0) {
            int realStock = stock - 1;
            stringRedisTemplate.opsForValue().set("stock", realStock + "");
            System.out.println("扣除库存成功，剩余库存：" + realStock);
        } else {
            System.out.println("扣除库存失败，库存不足");
        }
    }

    /**
     * 加上jvm级别的锁，多个线程来时，可以解决并发问题：
     * 但是在分布式环境下，还是会有并发问题：多个并发，轮询去分发请求
     */
    private void version2() throws Exception {
        synchronized (this) {
            int stock = Integer.parseInt(stringRedisTemplate.opsForValue().get("stock"));
            if (stock > 0) {
                int realStock = stock - 1;
                stringRedisTemplate.opsForValue().set("stock", realStock + "");
                System.out.println("扣除库存成功，剩余库存：" + realStock);
            } else {
                System.out.println("扣除库存失败，库存不足");
            }
        }
    }

    /**
     * 用setNX    http://doc.redisfans.com/
     * <p>
     * SETNX key value
     * 将 key 的值设为 value ，当且仅当 key 不存在。
     * 若给定的 key 已经存在，则 SETNX 不做任何动作。
     * 返回值：
     * 设置成功，返回 1 。
     * 设置失败，返回 0 。
     * <p>
     * <p>
     * 这样就可以解决分布式高并发问题：
     * <p>
     * 但是有问题：异常，死锁等；
     * 抛异常了，线程中断，key一直存在，则发生死锁；后边的减库存一直存在；
     */
    private void version3() throws Exception {
        String lockKey = "stock_key";
        Boolean result = stringRedisTemplate.opsForValue().setIfAbsent(lockKey, "stock_keyValue");
        if (!result) {
            System.out.println("拿不到锁，直接返回");
            return;
        }

        int stock = Integer.parseInt(stringRedisTemplate.opsForValue().get("stock"));
        if (stock > 0) {
            int realStock = stock - 1;
            stringRedisTemplate.opsForValue().set("stock", realStock + "");
            System.out.println("扣除库存成功，剩余库存：" + realStock);
        } else {
            System.out.println("扣除库存失败，库存不足");
        }
        Boolean delRst = stringRedisTemplate.delete(lockKey);
        System.out.println("删除锁:" + (delRst ? "成功" : "失败"));
    }


    /**
     * 解决死锁方案，try  finally:
     * 问题：代码执行到半中间，挂了，比如重启服务（睡眠10s模拟），finally没有执行到，又发生死锁；
     */
    private void version4() throws Exception {
        String lockKey = "stock_key";
        try {
            Boolean result = stringRedisTemplate.opsForValue().setIfAbsent(lockKey, "stock_keyValue");
            if (!result) {
                System.out.println("拿不到锁，直接返回");
                return;
            }
            //执行到这里服务挂了
            int stock = Integer.parseInt(stringRedisTemplate.opsForValue().get("stock"));
            if (stock > 0) {
                int realStock = stock - 1;
                stringRedisTemplate.opsForValue().set("stock", realStock + "");
                System.out.println("扣除库存成功，剩余库存：" + realStock);
            } else {
                System.out.println("扣除库存失败，库存不足");
            }
        } finally {
            Boolean delRst = stringRedisTemplate.delete(lockKey);
            System.out.println("删除锁:" + (delRst ? "成功" : "失败"));
        }
    }

    /**
     * 解决方案，锁添加超时时间；
     * 如果执行到了setIfAbsent，挂了，失效时间没执行，又发生死锁；
     */
    private void version5() throws Exception {
        String lockKey = "stock_key";
        try {
            Boolean result = stringRedisTemplate.opsForValue().setIfAbsent(lockKey, "stock_keyValue");
            //执行到这里服务挂了
            stringRedisTemplate.expire(lockKey, 10, TimeUnit.SECONDS);
            if (!result) {
                System.out.println("拿不到锁，直接返回");
                return;
            }

            int stock = Integer.parseInt(stringRedisTemplate.opsForValue().get("stock"));
            if (stock > 0) {
                int realStock = stock - 1;
                stringRedisTemplate.opsForValue().set("stock", realStock + "");
                System.out.println("扣除库存成功，剩余库存：" + realStock);
            } else {
                System.out.println("扣除库存失败，库存不足");
            }
        } finally {
            Boolean delRst = stringRedisTemplate.delete(lockKey);
            System.out.println("删除锁:" + (delRst ? "成功" : "失败"));
        }
    }


    /**
     * 解决方案：高版本redis把两行代码，原子化，不会被打断，都要执行；
     * <p>
     * 还会存在问题：线程B的锁，被线程A释放到，导致锁失效；
     * 过期时间10秒，第一个线程要执行15秒，执行10秒后超时，第二线程过来，需要执行10秒，
     * 15秒后，第一个线程释放第二个线程的锁，导致锁失效；
     */
    private void version6() throws Exception {
        String lockKey = "stock_key";
        try {
            //解决方案：高版本redis把两行代码，原子化，不会被打断，都要执行；
            Boolean result = stringRedisTemplate.opsForValue().setIfAbsent(lockKey, "stock_keyValue", 10, TimeUnit.SECONDS);
            if (!result) {
                System.out.println("拿不到锁，直接返回");
                return;
            }

            int stock = Integer.parseInt(stringRedisTemplate.opsForValue().get("stock"));
            if (stock > 0) {
                int realStock = stock - 1;
                stringRedisTemplate.opsForValue().set("stock", realStock + "");
                System.out.println("扣除库存成功，剩余库存：" + realStock);
            } else {
                System.out.println("扣除库存失败，库存不足");
            }
        } finally {
            Boolean delRst = stringRedisTemplate.delete(lockKey);
            System.out.println("删除锁:" + (delRst ? "成功" : "失败"));
        }
    }

    /**
     * 解决方式：通过stock_keyValue隔离，可以解决的问题
     * 但是超时问题还是无法解决；还是可能存在并发执行原子的代码段问题；(极限例子：失效时间1s,多个线程过来，多次操作数据库，有数据库io，导致多个线程操作，还存在原子代码段问题)
     * lockKey是一样的，多个请求针对同一个lockKey会被限制住
     */
    private void version7() throws Exception {
        String lockKey = "stock_key";
        String clientId = UUID.randomUUID().toString();
        try {
            //解决方案：高版本redis把两行代码，原子化，不会被打断，都要执行；
            Boolean result = stringRedisTemplate.opsForValue().setIfAbsent(lockKey, clientId, 10, TimeUnit.SECONDS);
            if (!result) {
                System.out.println("拿不到锁，直接返回");
                return;
            }

            int stock = Integer.parseInt(stringRedisTemplate.opsForValue().get("stock"));
            if (stock > 0) {
                int realStock = stock - 1;
                stringRedisTemplate.opsForValue().set("stock", realStock + "");
                System.out.println("扣除库存成功，剩余库存：" + realStock);
            } else {
                System.out.println("扣除库存失败，库存不足");
            }
        } finally {
            if (clientId.equals(stringRedisTemplate.opsForValue().get(lockKey))) {
                Boolean delRst = stringRedisTemplate.delete(lockKey);
                System.out.println("删除锁:" + (delRst ? "成功" : "失败"));
            }
        }
    }

    /**
     * 使用，三行代码搞定，底层帮我实现了刚才的逻辑；
     * lock.tryLock():默认30秒超时，后台开线程10秒，续命；
     * 时间可以自己设置；
     * lock.tryLock(60，TimeUnil.SECONDS);设置60秒超时，开启的线程，可能20秒续命一次；
     */
    private void version8() throws Exception {
        String lockKey = "stock_key";
        RLock lock = redisson.getLock(lockKey);
        try {
            lock.tryLock();

            int stock = Integer.parseInt(stringRedisTemplate.opsForValue().get("stock"));
            if (stock > 0) {
                int realStock = stock - 1;
                stringRedisTemplate.opsForValue().set("stock", realStock + "");
                System.out.println("扣除库存成功，剩余库存：" + realStock);
            } else {
                System.out.println("扣除库存失败，库存不足");
            }
        } finally {
            lock.unlock();
        }
    }
}

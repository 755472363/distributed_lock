package com.hyk.distributed_lock.demo;

import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

@RestController
public class IndexController {

    @Autowired
    private RedissonClient redisson;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    private void setStock() throws Exception {
        Integer stock = Integer.parseInt(stringRedisTemplate.opsForValue().get("stock_hyk"));
        if(stock != null && stock <= 0){
            stringRedisTemplate.opsForValue().set("stock_hyk", "10");
        }else {
            System.out.println("当前库存："+stock);
        }
    }

    @RequestMapping("/deduct_stock")
    public String deductStock() throws Exception {
        setStock();
        version8();
//        version2();
        return "end";
    }

    /**
     * redis中，库存有10个,减库存逻辑
     * 最初版本，多线程情况下，会有线程并发问题：
     */
    private void version1() throws Exception {
        int stock = Integer.parseInt(stringRedisTemplate.opsForValue().get("stock_hyk"));
        if (stock > 0) {
            int realStock = stock - 1;
            stringRedisTemplate.opsForValue().set("stock_hyk", realStock + "");
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
            int stock = Integer.parseInt(stringRedisTemplate.opsForValue().get("stock_hyk"));
            if (stock > 0) {
                int realStock = stock - 1;
                stringRedisTemplate.opsForValue().set("stock_hyk", realStock + "");
                System.out.println("扣除库存成功，剩余库存：" + realStock);
            } else {
                System.out.println("扣除库存失败，库存不足");
            }
        }
    }

    /**
     * 从这里，进入redis实现分布式锁
     * 遇到了各种各样的问题，然后不断解决，最后得到了一个比较完美的版本
     * 接下来每个版本，我们自己要思考一下有啥问题，然后咋解决
     * /

    /**
     * 用setNX    http://doc.redisfans.com/
     * 这样就可以解决分布式高并发问题：
     *
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

        int stock = Integer.parseInt(stringRedisTemplate.opsForValue().get("stock_hyk"));
        if (stock > 0) {
            int realStock = stock - 1;
            stringRedisTemplate.opsForValue().set("stock_hyk", realStock + "");
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
     *
     * 释放锁 和 countDownLatch   latch.countDown();需要放到在finally里，避免异常导致的死锁，线程阻塞
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
            int stock = Integer.parseInt(stringRedisTemplate.opsForValue().get("stock_hyk"));
            if (stock > 0) {
                int realStock = stock - 1;
                stringRedisTemplate.opsForValue().set("stock_hyk", realStock + "");
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

            int stock = Integer.parseInt(stringRedisTemplate.opsForValue().get("stock_hyk"));
            if (stock > 0) {
                int realStock = stock - 1;
                stringRedisTemplate.opsForValue().set("stock_hyk", realStock + "");
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
     * 过期时间10秒，第一个线程要执行15秒，执行10秒后超时，5秒后第二线程过来，需要执行15秒，
     * 15秒后，第一个线程释放第二个线程的锁，导致锁失效；
     * 等到第二线程执行完，又释放了后一个线程的锁，循环往复，恶心循环
     *
     * 这里可以说是两个线程，也可以说是两台机器
     * 我们可以将锁的value设置成一个客户端的唯一值，比如生成一个UUID，删除的时候判断一下这个值是否是自己生成，这样就可以避免把其他服务加的锁删掉。
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

            int stock = Integer.parseInt(stringRedisTemplate.opsForValue().get("stock_hyk"));
            if (stock > 0) {
                int realStock = stock - 1;
                stringRedisTemplate.opsForValue().set("stock_hyk", realStock + "");
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
     * 这样只是保证自己的锁不被别人删掉，但是这个判断再删除的操作也不是原子操作，同时超时的问题还是没有解决(极限例子：失效时间1s,多个线程过来，多次操作数据库，有数据库io，导致多个线程操作，还存在原子代码段问题)
     * 问题：随机的clientId，多个请求可以进来，导致锁失效；
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

            int stock = Integer.parseInt(stringRedisTemplate.opsForValue().get("stock_hyk"));
            if (stock > 0) {
                int realStock = stock - 1;
                stringRedisTemplate.opsForValue().set("stock_hyk", realStock + "");
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
     * 解决方案：
     * 开启一个分线程，监视定时器，比如每5秒lock是否还存在，存在重置为10秒，给lockKey续命，直到锁被释放；
     *
     * 问题2:如果锁续命，释放锁时候的对比，是不是就不需要了
     * 因为锁一直续命，其它线程进不来，不会从在交叉，不需要对比；
     * 金顺给的场景：如果主节点拿到锁挂了，还没同步到从节点；线程A还在继续执行；
     * 线程b来了，从节点获取不到锁，继续执行；线程A执行完毕，释放了线程B到锁，导致锁失效；
     */
    private void version8() throws Exception {
        String lockKey = "stock_key";
        String clientId = UUID.randomUUID().toString();
        try {
            //解决方案：高版本redis把两行代码，原子化，不会被打断，都要执行；
            Boolean result = stringRedisTemplate.opsForValue().setIfAbsent(lockKey, clientId, 10, TimeUnit.SECONDS);
            if (!result) {
                System.out.println("拿不到锁，直接返回");
                return;
            }
            //开启一个分线程，监视定时器，比如每5秒lock是否还存在，存在重置为10秒，给lockKey续命，直到锁被释放；
            //逻辑省略；看源码；

            int stock = Integer.parseInt(stringRedisTemplate.opsForValue().get("stock_hyk"));
            if (stock > 0) {
                int realStock = stock - 1;
                stringRedisTemplate.opsForValue().set("stock_hyk", realStock + "");
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
     * 如果我们自己按照这个逻辑去实现，有可能还会有很多bug。Redisson已经帮我们很好的实现了分布式锁。配置好之后，使用就像使用java的lock一样。原理就和上述差不多。
     *
     * 使用，三行代码搞定，底层帮我实现了刚才的逻辑；
     * lock.tryLock():默认30秒超时，后台开线程10秒，续命；
     * 时间可以自己设置；
     * lock.tryLock(60，TimeUnil.SECONDS);设置60秒超时，开启的线程，可能20秒续命一次；
     */
    private void versionRedission() throws Exception {
        String lockKey = "stock_key";
        RLock lock = redisson.getLock(lockKey);
        try {
            lock.tryLock();
            lock.tryLock(12L,12L,TimeUnit.SECONDS);

            int stock = Integer.parseInt(stringRedisTemplate.opsForValue().get("stock_hyk"));
            if (stock > 0) {
                int realStock = stock - 1;
                stringRedisTemplate.opsForValue().set("stock_hyk", realStock + "");
                System.out.println("扣除库存成功，剩余库存：" + realStock);
            } else {
                System.out.println("扣除库存失败，库存不足");
            }
        } finally {
            lock.unlock();
        }
    }









    /* 结束语：
     * 虽然看起来已经很完善了，但是还有一点点问题如果哨兵模式，或者集群模式，锁加载master上面，还未同步到slave的时候，master挂了，这个重新选举，新的master上面是没有加锁的。
     *
     *
     * 不过这种几率已经很小很小了，如果是在要求强一致性，那么就只有选择zookeeper来实现，因为zookeeper是强一致性的，它是多数节点数据都同步好了才返回。
     * Master挂了，选举也是在数据一致的节点中，因此重新选上来leader肯定是有锁的。当然ZK的性能肯定就没有redis的高了，怎么选择还是看自己业务是否允许。
     */
}

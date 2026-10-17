package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.BLOG_LIKED_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {
    private final StringRedisTemplate stringRedisTemplate;
    private final IUserService iUserService;

    public BlogServiceImpl(StringRedisTemplate stringRedisTemplate, IUserService iUserService) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.iUserService = iUserService;
    }
    
    public Result queryBlogById(Long id) {
        // 1. 查笔记
        Blog blog = getById(id);
        // 2. 不存在返回失败
        if (blog == null) {
            return Result.fail("笔记不存在");
        }
        // 3. 填充作者信息（name / icon）
        queryBlogUser(blog);
        // 4. 判断当前用户是否点赞
        isBlogLiked(blog);

        return Result.ok(blog);
    }


    /** 查询笔记详情 */

    @Override
    public Result likeBlog(Long id) {
        // 1. 取当前用户 id
        Long userId = UserHolder.getUser().getId();
        // 2. key = "blog:liked:" + id
        String key = BLOG_LIKED_KEY + id;
        // 3. ZSCORE 判断是否已点赞（score == null 表示没点过）
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        if (score == null){
            // 4. 没点过 → 点赞
            boolean isSuccess = update().setSql("liked = liked + 1").eq("id", id).update();
            if (isSuccess){
                stringRedisTemplate.opsForZSet().add(key, userId.toString(), System.currentTimeMillis());
            }
        }else{
            // 5. 点过   取消点赞 数据库点赞数-1
            boolean isSuccess = update().setSql("liked = liked - 1").eq("id", id).update();
            //4.2 把用户从Redis的set集合移除
            if(isSuccess){
                stringRedisTemplate.opsForZSet().remove(key, userId.toString());
            }
        }
        return Result.ok();
    }
    /** 点赞排行 TOP5 */
    @Override
    public Result queryBlogLikes(Long id) {
        String key = BLOG_LIKED_KEY + id;
        // 1. ZRANGE key 0 4  取 TOP5 用户 id（String 集合）
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(key, 0, 4);
        // 2. 空则返回空列表
        if (top5 == null || top5.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }
        // 3. 转成 List<Long>
        List<Long> ids = top5.stream().map(Long::valueOf).collect(Collectors.toList());
        String idStr = StrUtil.join(",", ids);
        // 4. 查用户表：WHERE id IN (...) ORDER BY FIELD(id, ...)
        List<UserDTO> userDTOS = iUserService.query()
                .in("id", ids).last("ORDER BY FIELD(id, " + idStr + ")").list()
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        // 5. 转 UserDTO 返回
        return Result.ok(userDTOS);
    }

    /** 填充笔记的作者信息和当前用户点赞状态（列表场景复用） */
    @Override
    public void fillBlogInfo(Blog blog) {
        queryBlogUser(blog);
        isBlogLiked(blog);
    }

    /** 查询作者信息 */
    private void queryBlogUser(Blog blog) {
        // 1. userId
        Long userId = blog.getUserId();
        // 2. userService.getById
        User user = iUserService.getById(userId);
        // 3. 填充 name / icon
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());

    }
    /** 是否点赞 */
    private void isBlogLiked(Blog blog) {
        // 1.获取登录用户
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            // 用户未登录，无需查询是否点赞
            return;
        }
        Long userId = user.getId();
        // 2.判断当前登录用户是否已经点赞
        String key = "blog:liked:" + blog.getId();
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        blog.setIsLike(score != null);
    }
}

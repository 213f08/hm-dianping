package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.entity.Blog;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IBlogService extends IService<Blog> {
    /** 查询笔记详情（含作者信息、当前用户是否点赞） */
    Result queryBlogById(Long id);

    /** 点赞 / 取消点赞 */
    Result likeBlog(Long id);

    /** 查询点赞 TOP5 用户 */
    Result queryBlogLikes(Long id);

    /** 填充笔记的作者信息和当前用户点赞状态（供列表场景复用） */
    void fillBlogInfo(Blog blog);
}

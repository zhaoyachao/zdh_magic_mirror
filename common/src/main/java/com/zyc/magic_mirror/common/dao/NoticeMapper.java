package com.zyc.magic_mirror.common.dao;

import com.zyc.magic_mirror.common.entity.NoticeInfo;
import org.apache.ibatis.annotations.Insert;

public interface NoticeMapper {
    @Insert({ "insert into notice_info (msg_type, msg_title,msg_url, msg, is_see, owner, create_time, update_time) values(#{msg_type}, #{msg_title}, #{msg_url}, #{ msg}, #{is_see}, #{owner}, #{create_time}, #{update_time})" })
    public int insert(NoticeInfo notice_info);
}

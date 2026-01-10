package com.zyc.magic_mirror.plugin.touch.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder
public class PushxBaseResponse {
    private Integer code;
    private String msg;
    private Object data;
    private Object third_party_code;
    private String third_party_message;

    public boolean isSuccess(){
        return this.code == 0;
    }
}

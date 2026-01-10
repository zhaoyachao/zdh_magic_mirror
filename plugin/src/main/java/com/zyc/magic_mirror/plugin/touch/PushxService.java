package com.zyc.magic_mirror.plugin.touch;

import com.zyc.magic_mirror.plugin.touch.entity.PushxBaseResponse;
import com.zyc.magic_mirror.plugin.touch.entity.PushxTemplateResponse;

import java.util.List;
import java.util.Map;

public interface PushxService {

    public PushxBaseResponse send(List<String> pushServers, String template_id, String account, String account_type, Map<String, Object> param);

    public PushxTemplateResponse getTemplate(String templateId);
}

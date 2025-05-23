package com.zyc.magic_mirror.plugin.calculate.impl;

import com.google.common.collect.Sets;
import com.zyc.magic_mirror.common.entity.DataPipe;
import com.zyc.magic_mirror.common.util.Const;
import com.zyc.magic_mirror.common.util.JsonUtil;
import com.zyc.magic_mirror.plugin.calculate.IdMappingEngine;
import org.apache.commons.lang3.StringUtils;
import org.redisson.Redisson;
import org.redisson.api.RKeys;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;

import java.util.*;

/**
 * redis, idmapping
 * 适用于可变动的场景
 */
public class RedisIdMappingEngineImpl implements IdMappingEngine {

    public static Map<String, RedisConf> redisConfMap = new HashMap<>();

    private String id_mapping_code;

    public RedisIdMappingEngineImpl(String id_mapping_code) throws Exception {
        this.id_mapping_code = id_mapping_code;
        if(!redisConfMap.containsKey(id_mapping_code)){
            //检查是否有默认引擎
            if(redisConfMap.containsKey("default")){
                return ;
            }
            throw new Exception("redis引擎无法找到对应的id_mapping配置, id_mapping_code: "+id_mapping_code);
        }
    }

    @Override
    public List<String> get() throws Exception {
        RedissonClient redissonClient = redisConfMap.getOrDefault(id_mapping_code,  redisConfMap.get("default")).redisson();
        try{
            List<String> list = new ArrayList<>();
            //根据id_mapping_code找到对应配置


            RKeys rKeys = redissonClient.getKeys();
            Iterator<String> iterator = rKeys.getKeysByPattern("id_mapping:"+id_mapping_code+":*").iterator();

            while (iterator.hasNext()){
                String key = iterator.next();
                String value = redissonClient.getBucket(key).get().toString();
                list.add(key+","+value);
            }
            return list;
        }catch (Exception e){
            throw e;
        }finally {
            redissonClient.shutdown();
        }

    }

    @Override
    public IdMappingResult getMap(Collection<DataPipe> rs) throws Exception {
        RedissonClient redissonClient = redisConfMap.getOrDefault(id_mapping_code,  redisConfMap.get("default")).redisson();
        try{
            IdMappingResult idMappingResult = new IdMappingResult();
            Set<DataPipe> id_map_rs = Sets.newHashSet();
            Set<DataPipe> id_map_rs_error = Sets.newHashSet();


            for (DataPipe r: rs){
                Object value = redissonClient.getBucket(r.getUdata()).get();

                if(value != null && !StringUtils.isEmpty(value.toString())){
                    Map<String, Object> stringObjectMap = JsonUtil.toJavaMap(r.getExt());
                    stringObjectMap.put("mapping_data", r.getUdata()+","+value.toString());
                    r.setUdata(value.toString());
                    id_map_rs.add(r);
                } else{
                    r.setStatus(Const.FILE_STATUS_FAIL);
                    r.setStatus_desc("id mapping error");
                    id_map_rs_error.add(r);
                }
            }

            idMappingResult.setRs(id_map_rs);
            idMappingResult.setRs_error(id_map_rs_error);

            return idMappingResult;
        }catch (Exception e){
            throw e;
        }finally {
            redissonClient.shutdown();
        }

    }

    public static class RedisConf{
        private String url;

        private String passwd;

        private String mode;

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getPasswd() {
            return passwd;
        }

        public void setPasswd(String passwd) {
            this.passwd = passwd;
        }

        public String getMode() {
            return mode;
        }

        public void setMode(String mode) {
            this.mode = mode;
        }

        public RedissonClient redisson(){
            if (StringUtils.isEmpty(passwd)) {
                passwd = null;
            }

            Config config = new Config();
            if (mode.contains("cluster")) {
                config.useClusterServers().addNodeAddress(new String[]{"redis://" +url}).setPassword(passwd);
            } else {
                config.useSingleServer().setAddress("redis://" + url).setPassword(passwd);
            }

            config.setCodec(StringCodec.INSTANCE);
            RedissonClient redisson = Redisson.create(config);
            return redisson;
        }
    }
}

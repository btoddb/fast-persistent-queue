package com.btoddb.fastpersitentqueue.eventbus;

/*
 * #%L
 * fast-persistent-queue
 * %%
 * Copyright (C) 2014 btoddb.com
 * %%
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 * #L%
 */

import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;


/**
 *
 */
public class Config {
    private static Logger logger = LoggerFactory.getLogger(Config.class);

    String configFilename;

    ObjectMapper objectMapper;

    Collection<FpqCatcher> catchers = new HashSet<>();
    Map<String, PlunkerRunner> plunkers = new HashMap<>();
    Collection<FpqRouter> routers = new HashSet<>();


    public Config() {
        objectMapper = new ObjectMapper();
//        objectMapper.setInjectableValues(new InjectableValues() {
//            @Override
//            public Object findInjectableValue(Object valueId, DeserializationContext ctxt, BeanProperty forProperty, Object beanInstance) {
//                return null;
//            }
//        })
    }

    public static Config create(String configFilename) throws FileNotFoundException {
        Yaml yaml = new Yaml(new Constructor(Config.class));
        Config config;
        FileInputStream inStream = new FileInputStream(configFilename);
        try {
            config = (Config) yaml.load(inStream);
            config.configFilename = configFilename;
        }
        finally {
            try {
                inStream.close();
            }
            catch (IOException e) {
                logger.error("exception while closing config file", e);
            }
        }

        return config;
    }

    public String getConfigFilename() {
        return configFilename;
    }

    public Collection<FpqRouter> getRouters() {
        return routers;
    }

    public void setRouters(Collection<FpqRouter> routers) {
        this.routers = routers;
    }

    public Collection<FpqCatcher> getCatchers() {
        return catchers;
    }

    public void setCatchers(Collection<FpqCatcher> catchers) {
        this.catchers = catchers;
    }

    public Map<String, PlunkerRunner> getPlunkers() {
        return plunkers;
    }

    public void setPlunkers(Map<String, PlunkerRunner> plunkerMap) {
        this.plunkers = plunkerMap;
    }

    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    public void setObjectMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pinterest.secor.util;

import java.lang.reflect.Constructor;

import com.pinterest.secor.common.SecorConfig;

/**
 * ReflectionUtil implements utility methods to construct objects of classes
 * specified by name.
 * 
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class ReflectionUtil {
	public static Object createMessageParser(String className,
			SecorConfig config) throws Exception {
		Class<?> clazz = Class.forName(className);

		// Search for an "appropriate" constructor.
		for (Constructor<?> ctor : clazz.getConstructors()) {
			Class<?>[] paramTypes = ctor.getParameterTypes();

			// If the arity matches, let's use it.
			if (paramTypes.length == 1) {
				Object[] args = { config };
				return ctor.newInstance(args);
			}
		}
		throw new IllegalArgumentException("Class not found " + className);
	}

	public static Object createStorageFactory(String className,
			SecorConfig config) throws Exception {
		Class<?> clazz = Class.forName(className);

		return clazz.newInstance();
	}
}

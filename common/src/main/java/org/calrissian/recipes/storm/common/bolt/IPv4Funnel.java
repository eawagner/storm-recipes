/*
 * Copyright (C) 2013 The Calrissian Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.calrissian.recipes.storm.common.bolt;

import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import org.calrissian.mango.domain.IPv4;

import java.io.Serializable;

/**
 * Date: 10/31/12
 * Time: 2:04 PM
 */
public class IPv4Funnel implements Funnel<IPv4>, Serializable {

    @Override
    public void funnel(IPv4 iPv4, PrimitiveSink primitiveSink) {
        primitiveSink.putLong(iPv4.getValue());
    }
}

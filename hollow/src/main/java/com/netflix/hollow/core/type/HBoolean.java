/*
 *
 *  Copyright 2017 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.hollow.core.type;

import com.netflix.hollow.api.custom.HollowAPI;
import com.netflix.hollow.api.objects.HollowObject;
import com.netflix.hollow.core.type.delegate.BooleanDelegate;

public class HBoolean extends HollowObject {

    public HBoolean(BooleanDelegate delegate, int ordinal) {
        super(delegate, ordinal);
    }

    public boolean getValue() {
        return delegate().getValue(ordinal);
    }

    public Boolean getValueBoxed() {
        return delegate().getValueBoxed(ordinal);
    }

    public HollowAPI api() {
        return typeApi().getAPI();
    }

    public BooleanTypeAPI typeApi() {
        return delegate().getTypeAPI();
    }

    protected BooleanDelegate delegate() {
        return (BooleanDelegate)delegate;
    }

}
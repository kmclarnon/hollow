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
package com.netflix.hollow.core.type.delegate;

import com.netflix.hollow.api.objects.delegate.HollowObjectAbstractDelegate;
import com.netflix.hollow.core.read.dataaccess.HollowObjectTypeDataAccess;
import com.netflix.hollow.core.schema.HollowObjectSchema;
import com.netflix.hollow.core.type.IntegerTypeAPI;

public class IntegerDelegateLookupImpl extends HollowObjectAbstractDelegate implements IntegerDelegate {

    private final IntegerTypeAPI typeAPI;

    public IntegerDelegateLookupImpl(IntegerTypeAPI typeAPI) {
        this.typeAPI = typeAPI;
    }

    @Override
    public int getValue(int ordinal) {
        return typeAPI.getValue(ordinal);
    }

    @Override
    public Integer getValueBoxed(int ordinal) {
        return typeAPI.getValueBoxed(ordinal);
    }

    @Override
    public IntegerTypeAPI getTypeAPI() {
        return typeAPI;
    }

    @Override
    public HollowObjectSchema getSchema() {
        return typeAPI.getTypeDataAccess().getSchema();
    }

    @Override
    public HollowObjectTypeDataAccess getTypeDataAccess() {
        return typeAPI.getTypeDataAccess();
    }

}
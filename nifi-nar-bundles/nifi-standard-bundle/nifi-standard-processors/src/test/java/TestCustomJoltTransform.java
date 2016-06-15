import com.bazaarvoice.jolt.Chainr;

import com.bazaarvoice.jolt.SpecDriven;
import com.bazaarvoice.jolt.Transform;
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class TestCustomJoltTransform implements SpecDriven,Transform {

    final private Transform customTransform;

    public TestCustomJoltTransform(Object specJson) {
        this.customTransform = Chainr.fromSpec(specJson);
    }

    @Override
    public Object transform(Object o) {
        return customTransform.transform(o);
    }

    public static void main(String[] args) {
        System.out.println("This is a Test Custom Transform");
    }

}

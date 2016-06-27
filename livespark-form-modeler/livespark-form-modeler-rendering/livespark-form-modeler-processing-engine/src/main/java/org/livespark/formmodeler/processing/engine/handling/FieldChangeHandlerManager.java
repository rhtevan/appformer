/*
 * Copyright 2016 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.livespark.formmodeler.processing.engine.handling;

public interface FieldChangeHandlerManager {
    void registerField( String fieldName );

    void registerField( String fieldName, boolean validateOnChange );

    void addFieldChangeHandler( FieldChangeHandler changeHandler );

    void addFieldChangeHandler( String fieldName, FieldChangeHandler changeHandler );

    void processFieldChange( String fieldName, Object newValue, Object model );

    void clear();

    void setValidator( FormValidator validator );
}
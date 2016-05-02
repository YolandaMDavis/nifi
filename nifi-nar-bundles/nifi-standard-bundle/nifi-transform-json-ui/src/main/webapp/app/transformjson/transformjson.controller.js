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
'use strict';

angular.module('standardUI')
    .controller('TransformJsonController', function ($scope, $state, $q, TransformJsonService, ProcessorService) {

        $scope.processorId = "";
        $scope.clientId = "";
        $scope.revisionId = "";
        $scope.editors = [];
        $scope.specEditor = {};
        $scope.jsonSpec = '';
        $scope.transform = '';
        $scope.transformOptions = [];
        $scope.jsonInput = '';
        $scope.jsonOutput = '';
        $scope.sortOutput = false;
        $scope.validObj = {};
        $scope.error = '';
        $scope.disableCSS = "";
        $scope.saveStatus = "";

        $scope.clearError = function(){
            $scope.error = '';
        }

        $scope.showError = function(message,detail){
            $scope.error = message;
            console.log('Error received:', detail);
        }

        $scope.addEditors = function(_editor){
            $scope.editors.push(_editor);
        };

        $scope.initSpecEditor = function(_editor){
            $scope.addEditors(_editor);
            $scope.specEditor = _editor;
        };

        $scope.editorProperties = {
            lineNumbers: true,
            gutters: ['CodeMirror-lint-markers'],
            mode: 'application/json',
            lint: true,
            onLoad: $scope.initSpecEditor
        };

        $scope.inputProperties = {
            lineNumbers: true,
            gutters: ['CodeMirror-lint-markers'],
            mode: 'application/json',
            lint: true,
            onLoad: $scope.addEditors
        };

        $scope.outputProperties = {
            lineNumbers: true,
            gutters: ['CodeMirror-lint-markers'],
            mode: 'application/json',
            lint: false,
            readOnly: true,
            onLoad: $scope.addEditors
        };

        $scope.hasEditorErrors = function(){
            for(var editor in $scope.editors){
                var markers = $scope.editors[editor].getDoc().getAllMarks();
                if(markers != null && markers.length > 0){
                    return true;
                }
            }
            return false;
        };

        $scope.toggleEditor = function(editor,transform){

            if(transform == 'jolt-transform-sort'){
                editor.setOption("readOnly","nocursor");
                $scope.disableCSS = "trans";
            }
            else{
                editor.setOption("readOnly",false);
                $scope.disableCSS = "";
            }

        }

        $scope.getJoltSpec = function(transform,jsonSpec,jsonInput){

            return  {
                "transform": transform,
                "specification" : jsonSpec,
                "input" : jsonInput
            };
        };

        $scope.getProperties = function(transform,jsonSpec){

            return {
                "jolt-transform" : transform != "" ? transform : null,
                "jolt-spec": jsonSpec != "" ? jsonSpec : null
            };

        }

        $scope.validateJson = function(jsonInput,jsonSpec,transform){

            var deferred = $q.defer();

            $scope.clearError();

            if( !$scope.hasEditorErrors() ){

                var joltSpec = $scope.getJoltSpec(transform,jsonSpec,jsonInput);

                TransformJsonService.validate(joltSpec).then(function(response){
                    $scope.validObj = response.data;
                    deferred.resolve($scope.validObj);
                }).catch(function(response) {
                    $scope.showError("Error occurred during validation",response.statusText)
                    deferred.reject($scope.error);
                });

            }else{
                $scope.validObj = {"valid":false,"message":"JSON provided is not valid"};
                deferred.resolve($scope.validObj);
            }

            return deferred.promise;

        };

        $scope.transformJson = function(jsonInput,jsonSpec,transform){

            $scope.validateJson(jsonInput,jsonSpec,transform).then(function(response){

                var valid = response.data;

                if($scope.validObj.valid == true){

                    var joltSpec = $scope.getJoltSpec(transform,jsonSpec,jsonInput);

                    TransformJsonService.execute(joltSpec).then(function(response){

                            $scope.jsonOutput = js_beautify(response.data, {
                                'indent_size': 1,
                                'indent_char': '\t'
                            });

                        })
                        .catch(function(response) {
                            $scope.showError("Error occurred during transformation",response.statusText)
                        });

                }

            });

        };

        $scope.saveSpec = function(jsonInput,jsonSpec,transform,processorId,clientId,revisionId){

            $scope.clearError();

            var properties = $scope.getProperties(transform,jsonSpec);

            ProcessorService.setProperties(processorId,revisionId,clientId,properties)
                .then(function(response) {
                    var componentDetails = response.data;
                    $scope.saveStatus = "Changes saved successfully";
                })
                .catch(function(response) {
                    $scope.showError("Error occurred during save properties",response.statusText);
                });

        };


        $scope.initController = function(params){

            $scope.processorId = params.id;
            $scope.clientId = params.clientId;
            $scope.revisionId = params.revision;

            ProcessorService.getDetails($scope.processorId).then(function(response){
                    var details = response.data;

                    if(details['properties']['jolt-spec'] != null){
                        $scope.jsonSpec = details['properties']['jolt-spec'];
                    }

                    $scope.transform = details['properties']['jolt-transform'] ? details['properties']['jolt-transform'] :
                        details['descriptors']['jolt-transform']['defaultValue'] ;

                    $scope.transformOptions = details['descriptors']['jolt-transform']['allowableValues'];

                    $scope.toggleEditor($scope.specEditor,$scope.transform);

                })
                .catch(function(response) {
                    $scope.showError("Error occurred during processor detail retrieval.",response.statusText)
                });

        };

        $scope.initController($state.params);


    });

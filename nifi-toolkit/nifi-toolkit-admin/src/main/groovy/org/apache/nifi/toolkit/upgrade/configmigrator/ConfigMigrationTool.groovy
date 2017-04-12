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

package org.apache.nifi.toolkit.upgrade.configmigrator

import com.google.common.collect.Lists
import com.google.common.io.Files
import org.apache.nifi.toolkit.upgrade.AbstractUpgradeTool
import org.apache.nifi.toolkit.upgrade.util.UpgradeUtil
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.DefaultParser
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import org.apache.commons.cli.ParseException
import org.apache.commons.io.FileUtils
import org.apache.nifi.toolkit.upgrade.util.Version
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.nio.file.Paths

public class ConfigMigrationTool extends AbstractUpgradeTool{

    private static
    final String DEFAULT_DESCRIPTION = "This tool is used to update nifi configurations (including properties, configuration and xml files)." +
            " For keys or entries that exist in the upgraded configuration values from the previous configuration will be moved over.  Any previously existing keys" +
            " that are not in the upgraded file will be reported for review. Also any keys that are "
    private static final String HELP_ARG = "help"
    private static final String VERBOSE_ARG = "verbose"
    private static final String BOOTSTRAP_CONF_FILE_ARG = "bootstrapConfFile"
    private static final String NIFI_UPGRADE_DIR = "nifiUpgradeDirectory"
    private static final String OVERWRITE_ARG = "overwrite"
    private final static String SUPPORTED_MINIMUM_VERSION = "1.0.0"
    private final String RULES_DIR = getRulesDirectory()
    private boolean isOverwritten

    ConfigMigrationTool() {
        header = buildHeader(DEFAULT_DESCRIPTION)
        setup()
    }

    ConfigMigrationTool(final String description) {
        header = buildHeader(description)
        setup()
    }

    @Override
    protected Logger getLogger() {
        LoggerFactory.getLogger(ConfigMigrationTool.class)
    }

    protected String getRulesDirectory() {
        final ClassLoader cl = this.getClass().getClassLoader()
        cl.getResource("rules").path.replaceAll("%20"," ");
    }

    protected Options getOptions() {
        final Options options = new Options()
        options.addOption(Option.builder("h").longOpt(HELP_ARG).desc("Print help info").build())
        options.addOption(Option.builder("v").longOpt(VERBOSE_ARG).desc("Set mode to verbose (default is false)").build())
        options.addOption(Option.builder("b").longOpt(BOOTSTRAP_CONF_FILE_ARG).hasArg().desc("NiFi Bootstrap Configuration file").build())
        options.addOption(Option.builder("u").longOpt(NIFI_UPGRADE_DIR).hasArg().desc("NiFi upgrade directory containing new configuration files").build())
        options.addOption(Option.builder("o").longOpt(OVERWRITE_ARG).desc("Overwrite the existing nifi configuration directory with upgrade changes").build())
        options
    }

    String getRulesDirectoryName(final String currentVersion, final String upgradeVersion) {
        Version current = new Version(currentVersion.take(5).toString(),".")
        Version upgrade = new Version(upgradeVersion.take(5).toString(),".")
        File rulesDir = new File(rulesDirectory)
        List<File> rules = Lists.newArrayList(rulesDir.listFiles())
        List<Version> versions = rules.collect { new Version(it.name.substring(1,it.name.length()),"_")}
        versions.sort(Version.VERSION_COMPARATOR)
        List<Version> matches = versions.findAll { Version.VERSION_COMPARATOR.compare(it,upgrade) <= 0 && Version.VERSION_COMPARATOR.compare(it,current) == 1}

        if(matches.isEmpty()){
            null
        }else{
            Version matchVersion = matches.max(Version.VERSION_COMPARATOR)
            RULES_DIR + File.separator + "v" + matchVersion.toString()
        }

    }

    Boolean supportedVersion(final File script, final String currentVersion) {
        final Class ruleClass = new GroovyClassLoader(getClass().getClassLoader()).parseClass(script);
        final GroovyObject ruleObject = (GroovyObject) ruleClass.newInstance();
        ruleObject.invokeMethod("supportedVersion", [currentVersion])
    }

    byte[] migrateContent(final File script, final byte[] content, final byte[] upgradeContent) {
        final Class ruleClass = new GroovyClassLoader(getClass().getClassLoader()).parseClass(script);
        final GroovyObject ruleObject = (GroovyObject) ruleClass.newInstance();
        ruleObject.invokeMethod("migrate", [content, upgradeContent])
    }

    String getScriptRuleName(final String fileName) {
        fileName.replace(".", "-") + ".groovy"
    }

    void migrateConfigFiles(final File script, final File upgradeDir, final File file) {
        final String fileName = file.getName()

        final File[] upgradeFiles = upgradeDir.listFiles(new FilenameFilter() {
            @Override
            boolean accept(File dir, String name) {
                name.equals(fileName)
            }
        })

        if (upgradeFiles.size() <= 1) {

            if (isVerbose) {
                logger.info("Attempting to migrate file: " + fileName)
            }

            final File upgradeFile = upgradeFiles.size() == 1 ? upgradeFiles[0] : new File(upgradeDir.path + File.separator + fileName)
            final byte[] content = migrateContent(script, file.bytes, upgradeFile.exists() ? upgradeFile.bytes : new byte[0])

            if (this.isOverwritten) {
                Files.write(content, file)
            } else {
                Files.write(content, upgradeFile)
            }

        }

    }

    void run(final File nifiConfDir, final File nifiLibDir,final File nifiUpgradeConfigDir, final File nifiUpgradeLibDir) {

        final String nifiCurrentVersion = UpgradeUtil.getNiFiVersion(nifiConfDir,nifiLibDir)
        final String nifiUpgradeVersion = UpgradeUtil.getNiFiVersion(nifiUpgradeConfigDir,nifiUpgradeLibDir)

        if (nifiCurrentVersion == null) {
            throw new IllegalArgumentException("Could not determine current nifi version")
        }

        if (nifiUpgradeVersion == null) {
            throw new IllegalArgumentException("Could not determine upgrade nifi version")
        }

        final File[] nifiConfigFiles = nifiConfDir.listFiles()
        final String ruleDir = getRulesDirectoryName(nifiCurrentVersion,nifiUpgradeVersion)

        if(ruleDir != null) {

            nifiConfigFiles.each { file ->

                final String scriptName = getScriptRuleName(file.getName())
                final File script = new File(ruleDir + File.separator + scriptName)

                if (script.exists() && supportedVersion(script, nifiCurrentVersion)) {
                    migrateConfigFiles(script, nifiUpgradeConfigDir, file)
                } else {
                    logger.info("No migration rule exists for file: " + file.getName() + ".")

                    if(!this.isOverwritten){
                        logger.info("Copying file to upgrade directory.")
                    }
                    if(!file.isDirectory()) {
                        Files.write(file.bytes, Paths.get(nifiUpgradeConfigDir.path, file.name).toFile())
                    }else{
                        FileUtils.copyDirectoryToDirectory(file,nifiUpgradeConfigDir)
                    }
                }
            }

        }else{
            logger.info("No upgrade rules are required for these configurations.")
            if(!this.isOverwritten){
                logger.info("Copying configurations over to upgrade directory")
                nifiConfigFiles.each { file ->
                    if(file.isDirectory()){
                        FileUtils.copyDirectoryToDirectory(file, nifiUpgradeConfigDir)
                    }else {
                        FileUtils.copyFileToDirectory(file, nifiUpgradeConfigDir)
                    }
                }
            }
        }

    }

    void parse(final String[] args) throws ParseException, IllegalArgumentException {

        CommandLine commandLine = new DefaultParser().parse(options, args)

        if (commandLine.hasOption(HELP_ARG)) {
            printUsage(null)
        } else {
            if (commandLine.hasOption(BOOTSTRAP_CONF_FILE_ARG) && commandLine.hasOption(NIFI_UPGRADE_DIR)) {

                if (commandLine.hasOption(VERBOSE_ARG)) {
                    this.isVerbose = true;
                }

                String bootstrapConfFile = commandLine.getOptionValue(BOOTSTRAP_CONF_FILE_ARG)
                File bootstrapConf = Paths.get(bootstrapConfFile).toFile()

                if (!bootstrapConf.exists()) {
                    throw new IllegalArgumentException("NiFi Bootstrap File provided does not exist: " + bootstrapConfFile)
                }

                Properties bootstrapProperties = getBootstrapConf(Paths.get(bootstrapConfFile))
                File nifiConfDir = new File(getRelativeDirectory(bootstrapProperties.getProperty("conf.dir"), bootstrapConf.getParentFile().getParentFile().absolutePath))
                File nifiLibDir = new File(getRelativeDirectory(bootstrapProperties.getProperty("lib.dir"), bootstrapConf.getParentFile().getParentFile().absolutePath))
                final String nifiUpgDirString = commandLine.getOptionValue(NIFI_UPGRADE_DIR)
                final File nifiUpgradeConfDir = Paths.get(nifiUpgDirString,"conf").toFile()
                final File nifiUpgradeLibDir = Paths.get(nifiUpgDirString,"lib").toFile()

                if(supportedNiFiMinimumVersion(nifiConfDir.canonicalPath, nifiLibDir.canonicalPath, SUPPORTED_MINIMUM_VERSION) &&
                   supportedNiFiMinimumVersion(nifiUpgradeConfDir.canonicalPath, nifiUpgradeLibDir.canonicalPath, SUPPORTED_MINIMUM_VERSION)) {

                    //allow tool to update configuration directory if external from nifi's upgrade configuration directory
                    if(!nifiConfDir.absolutePath.equals(bootstrapConf.getParentFile().absolutePath) && commandLine.hasOption(OVERWRITE_ARG)){
                        this.isOverwritten = true
                    }

                    if (!nifiConfDir.exists() || !nifiConfDir.isDirectory()) {
                        throw new IllegalArgumentException("NiFi Configuration Directory provided is not valid: " + nifiConfDir.absolutePath)
                    }

                    if (!nifiUpgradeConfDir.exists() || !nifiUpgradeConfDir.isDirectory()) {
                        throw new IllegalArgumentException("Upgrade Configuration Directory provided is not valid: " + nifiUpgradeConfDir)
                    }

                    if (isVerbose) {
                        logger.info("Migrating configurations from {} to {}", nifiConfDir.absolutePath, nifiUpgradeConfDir.absolutePath)
                    }

                    run(nifiConfDir,nifiLibDir,nifiUpgradeConfDir,nifiUpgradeLibDir)

                    if (isVerbose) {
                        logger.info("Migration completed.")
                    }

                }else{
                    throw new UnsupportedOperationException("Config Migration Tool only supports NiFi version 1.0.0 and above")
                }

            } else if (!commandLine.hasOption(BOOTSTRAP_CONF_FILE_ARG)) {
                throw new ParseException("Missing -b option")
            } else {
                throw new ParseException("Missing -u option")
            }
        }

    }

    public static void main(String[] args) {
        ConfigMigrationTool tool = new ConfigMigrationTool()

        try {
            tool.parse(args)
        } catch (ParseException | IllegalArgumentException e) {
            tool.printUsage(e.getLocalizedMessage());
            System.exit(1)
        }

        System.exit(0)
    }

}
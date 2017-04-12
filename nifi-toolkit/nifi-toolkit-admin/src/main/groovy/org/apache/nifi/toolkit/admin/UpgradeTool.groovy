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
package org.apache.nifi.toolkit.admin

import com.google.common.collect.Lists
import com.sun.jersey.api.client.Client
import org.apache.nifi.toolkit.admin.client.NiFiClientUtil
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.DefaultParser
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import org.apache.commons.cli.ParseException
import org.apache.commons.compress.archivers.ArchiveEntry
import org.apache.commons.compress.archivers.ArchiveInputStream
import org.apache.commons.compress.archivers.ArchiveStreamFactory
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.commons.io.FilenameUtils
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.SystemUtils
import org.apache.nifi.properties.NiFiPropertiesLoader
import org.apache.nifi.toolkit.admin.client.ClientFactory
import org.apache.nifi.toolkit.admin.client.NiFiClientFactory
import org.apache.nifi.toolkit.admin.configmigrator.ConfigMigrationTool
import org.apache.nifi.toolkit.admin.filemanager.FileManagerTool
import org.apache.nifi.toolkit.admin.nodemanager.NodeManagerTool
import org.apache.nifi.toolkit.admin.notify.NotificationTool
import org.apache.nifi.util.NiFiProperties
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.TimeUnit

public class UpgradeTool extends AbstractAdminTool{

    private static final String DEFAULT_DESCRIPTION = "This tool is used to upgrade or rollback a nifi instance in standalone or cluster mode "
    private static final String HELP_ARG = "help"
    private static final String VERBOSE_ARG = "verbose"
    private static final String TYPE = "type"
    private static final String NOTIFY = "notify"
    private static final String NIFI_LINK = "nifiLink"
    private static final String NIFI_DIR = "nifiDir"
    private static final String NIFI_INSTALL_DIR = "nifiInstallDir"
    private static final String NIFI_ROLLBACK_DIR = "nifiRollbackDir"
    private static final String NIFI_PID_DIR = "nifiPidDir"
    private static final String BACKUP_DIR = "backupDir"
    private static final String UPGRADE_FILE = "upgradeFile"
    private final static String SUPPORTED_MINIMUM_VERSION = "1.0.0"
    private final ClientFactory clientFactory = new NiFiClientFactory()
    private final FileManagerTool fileManagerTool = new FileManagerTool()
    private final NodeManagerTool nodeManagerTool = new NodeManagerTool()
    private final static int DEFAULT_NOTIFY_TIME = 30

    UpgradeTool() {
        header = buildHeader(DEFAULT_DESCRIPTION)
        setup()
    }

    UpgradeTool(final String description){
        this.header = buildHeader(description)
        setup()
    }

    @Override
    protected Logger getLogger() {
        LoggerFactory.getLogger(UpgradeTool.class)
    }

    protected Options getOptions(){
        final Options options = new Options()
        options.addOption(Option.builder("h").longOpt(HELP_ARG).desc("Print help info").build())
        options.addOption(Option.builder("v").longOpt(VERBOSE_ARG).desc("Set mode to verbose (default is false)").build())
        options.addOption(Option.builder("t").longOpt(TYPE).hasArg().desc("File operation (install | backup | restore)").build())
        options.addOption(Option.builder("l").longOpt(NIFI_LINK).hasArg().desc("NiFi sym link to create or that currently points to current NiFi directory (only for Non-Windows Platforms)").build())
        options.addOption(Option.builder("d").longOpt(NIFI_DIR).hasArg().desc("Current NiFi directory").build())
        options.addOption(Option.builder("i").longOpt(NIFI_INSTALL_DIR).hasArg().desc("The parent directory where nifi upgrade should be installed").build())
        options.addOption(Option.builder("r").longOpt(NIFI_ROLLBACK_DIR).hasArg().desc("NiFi Rollback Directory (used with install or restore operation)").build())
        options.addOption(Option.builder("p").longOpt(NIFI_PID_DIR).hasArg().desc("NiFi process id directory").build())
        options.addOption(Option.builder("u").longOpt(UPGRADE_FILE).hasArg().desc("NiFi Upgrade File").build())
        options.addOption(Option.builder("b").longOpt(BACKUP_DIR).hasArg().desc("Backup NiFi Directory").build())
        options.addOption(Option.builder("n").longOpt(NOTIFY).hasArg().optionalArg(true).desc("Send a notification with subsequent delay in seconds").build())
        options
    }

    protected boolean validNiFiDirectory(String nifiDirectory){
        Path nifiPath = Paths.get(nifiDirectory)
        if(!Files.isDirectory(nifiPath) || Files.isSymbolicLink(nifiPath) || !Files.exists(Paths.get(nifiDirectory,"bin","nifi.sh"))){
            return false
        }
        else {
            return true
        }
    }

    NiFiProperties getNiFiProperties(String bootstrapConfDir){
        final File bootstrapConf = new File(bootstrapConfDir)
        Properties bootstrapProperties = getBootstrapConf(Paths.get(bootstrapConfDir))
        String nifiConfDir = getRelativeDirectory(bootstrapProperties.getProperty("conf.dir"), bootstrapConf.getCanonicalFile().getParentFile().getParentFile().getCanonicalPath())
        String nifiPropertiesFileName = nifiConfDir + File.separator +"nifi.properties"
        final String key = NiFiPropertiesLoader.extractKeyFromBootstrapFile(bootstrapConfDir)
        return NiFiPropertiesLoader.withKey(key).load(nifiPropertiesFileName)
    }

    protected void backupFiles(final String backupNiFiDirName, final String nifiLinkName){
        final List<String> params = Lists.newArrayList(["-o","backup","-b",backupNiFiDirName,"-c",nifiLinkName])

        if(isVerbose){
            params.add("-v")
        }

        fileManagerTool.parse(params as String[])
    }

    protected void installFiles(final String upgradeFileName, final String nifiLinkName, final String nifiInstallDirName){
        final List<String> params = Lists.newArrayList(["-o","install","-u",upgradeFileName,"-c",nifiLinkName,"-d",nifiInstallDirName,"-m"])
        if(isVerbose){
            params.add("-v")
        }
        fileManagerTool.parse(params as String[])
    }

    protected void restoreFiles(final String backupNiFiDirName, final String nifiLinkName, final String rollbackNiFiDirName){
        final List<String> params = Lists.newArrayList(["-o","restore","-b",backupNiFiDirName,"-c",nifiLinkName,"-r",rollbackNiFiDirName,"-m"] )
        if(isVerbose){
            params.add("-v")
        }
        fileManagerTool.parse(params as String[])
    }

    protected void migrateConfigFiles(final String upgradeRootDirName, final String bootstrapConfName){
        final ConfigMigrationTool configMigrationTool = new ConfigMigrationTool()
        final List<String> params = Lists.newArrayList(["-u",upgradeRootDirName,"-b",bootstrapConfName,"-o"] )
        if(isVerbose){
            params.add("-v")
        }
        configMigrationTool.parse(params as String[])
    }

    protected void notify(final String message, final String level, final String nifiLinkName, final String bootstrapConfName){
        final NotificationTool notificationTool = new NotificationTool()
        final List<String> params = Lists.newArrayList(["-d",nifiLinkName,"-b",bootstrapConfName,"-m",message,"-l",level]  )
        if(isVerbose){
            params.add("-v")
        }
        notificationTool.parse(clientFactory,params as String[])
    }

    protected void connectNode(final String nifiLinkName, final String bootstrapConfName,final List<String> activeUrls){
        final List<String> params = Lists.newArrayList(["-d",nifiLinkName,"-b",bootstrapConfName,"-o","connect","-u",activeUrls.join(",")] )

        if(isVerbose){
            params.add("-v")
        }

        nodeManagerTool.parse(clientFactory,params as String[])
    }

    protected void disconnectNode(final String nifiLinkName, final String bootstrapConfName, final List<String> activeUrls){
        final List<String> params = Lists.newArrayList(["-d",nifiLinkName,"-b",bootstrapConfName,"-o","disconnect","-u",activeUrls.join(",")])

        if(isVerbose){
            params.add("-v")
        }

        nodeManagerTool.parse(clientFactory,params as String[])
    }

    protected List<String> getActiveUrls(Client client, NiFiProperties nifiProperties){

        try {
            List<String> activeUrls = NiFiClientUtil.getActiveClusterUrls(client, nifiProperties)
            return activeUrls
        }catch (RuntimeException ex){
            if(isVerbose){
                logger.warn("Could not obtain cluster active urls: " + ex.message)
            }
        }
        null
    }

    protected void sendNotification(Integer notifyTime, String operation, String nifiLinkName, String bootstrapConfName){

        final InetAddress ia = InetAddress.getLocalHost();
        final StringBuilder message = new StringBuilder("Sending notification of ")
        message.append(operation)
        message.append(" on host ")
        message.append(ia.hostName)
        message.append(".")

        if(notifyTime != null && notifyTime < 0){
            message.append("  Will pause for")
            message.append(notifyTime)
            message.append(" seconds before starting")
            message.append(operation)
            message.append(" process")
        }

        try {
            notify(message.toString(),"WARN",nifiLinkName, bootstrapConfName)
            if (notifyTime != null && notifyTime > 0) {
                Thread.sleep(notifyTime * 1000)
            }
        }catch(Exception ex){
            if(isVerbose){
                logger.warn("Unable to send notification due to encountered exception: {} ",ex.message)
            }
        }
    }

    protected String getArchiveRootDirectory(final String upgradeFile) {

        String extension = FilenameUtils.getExtension(upgradeFile)
        InputStream fis = extension.equals("gz") ? new GzipCompressorInputStream(new FileInputStream(upgradeFile)) : new FileInputStream(upgradeFile)
        final ArchiveInputStream inputStream = new ArchiveStreamFactory().createArchiveInputStream(new BufferedInputStream(fis))
        ArchiveEntry entry = inputStream.nextEntry

        while (entry != null) {

            if(!StringUtils.isEmpty(entry.name)) {

                String archiveRootDir = entry.name.indexOf("" + File.separatorChar + "") > -1 ? entry.name.substring(0, entry.getName().indexOf("" + File.separatorChar + "")): entry.name
                if (archiveRootDir.toLowerCase().startsWith("nifi")) {
                    return archiveRootDir
                }
            }

            entry = inputStream.nextEntry
        }

        return "nifi_upgrade"

    }

    protected void executeWindowsCommand(String command, String workingDir){
        final Process process =  Runtime.getRuntime().exec(command,null,Paths.get(workingDir).toFile())
        process.getOutputStream().close();
    }

    protected void executeNiFiWindowsCommand(final String nifiDirName,final String command){

        if(command.equals("start")){
            final String startNiFiCommand = "cmd.exe /c start \"RunNiFi\" \"" + Paths.get(nifiDirName,"bin","run-nifi.bat").toString() + "\""

            if(isVerbose){
                logger.info("Starting NiFi with command: {}", startNiFiCommand)
            }

           executeWindowsCommand(startNiFiCommand,Paths.get(nifiDirName,"bin").toString())

        }else{

            final String stopRunNiFiCommand = "wmic process where \"commandline like '%org.apache.nifi.bootstrap.RunNiFi%'\" delete"
            final String stopNiFiCommand = "wmic process where \"commandline like '%org.apache.nifi.NiFi%'\" delete"

            if(isVerbose){
                logger.info("Stopping NiFi with command: {}",stopNiFiCommand)
            }
            executeWindowsCommand(stopRunNiFiCommand,nifiDirName)
            executeWindowsCommand(stopNiFiCommand,nifiDirName)
        }

    }

    protected void executeNiFiCommand(final String nifiDirName,final String command){

        if(isVerbose){
            logger.info("Executing " + command.toUpperCase() +" command for NiFi")
        }

        if(SystemUtils.IS_OS_WINDOWS){
            executeNiFiWindowsCommand(nifiDirName,command)
        }else{
            final ProcessBuilder pb = new ProcessBuilder(Paths.get(nifiDirName,"bin","nifi.sh").toString(), command.toLowerCase());
            Process p = pb.start();
            p.waitFor(60, TimeUnit.SECONDS)
        }

        if(isVerbose){
            logger.info(command.capitalize()+" command completed successfully")
        }

    }

    String install(final String upgradeFileName, final String nifiLinkName, final String nifiInstallDirName){
        final String bootstrapConf = Paths.get(nifiLinkName,"conf","bootstrap.conf").toString()
        String upgradeRootDirName = Paths.get(nifiInstallDirName,getArchiveRootDirectory(upgradeFileName)).toString()
        installFiles(upgradeFileName,nifiLinkName,nifiInstallDirName)
        migrateConfigFiles(upgradeRootDirName.toString(),bootstrapConf)

        if(Files.isSymbolicLink(Paths.get(nifiLinkName))) {
            Files.delete(Paths.get(nifiLinkName))
            Files.createSymbolicLink(Paths.get(nifiLinkName), Paths.get(upgradeRootDirName))
        }
        return upgradeRootDirName
    }

    void downgrade(final CommandLine commandLine, final String nifiLinkName,  final String bootstrapConf, final ClientFactory clientFactory){

        if(isVerbose){
            logger.info("Starting Downgrade")
        }

        final NiFiProperties nifiProperties = getNiFiProperties(bootstrapConf)
        final Client client = clientFactory.getClient(nifiProperties,nifiLinkName)
        List<String> activeUrls = getActiveUrls(client,nifiProperties)
        String backupNiFiDirName = null
        String rollbackNiFiDirName

        if(commandLine.hasOption(BACKUP_DIR)){
            backupNiFiDirName = commandLine.getOptionValue(BACKUP_DIR)
        }

        if(commandLine.hasOption(NIFI_ROLLBACK_DIR)){
            rollbackNiFiDirName = commandLine.getOptionValue(NIFI_ROLLBACK_DIR)
        }else{
            throw new ParseException("Missing -r option")
        }

        if(commandLine.hasOption(NOTIFY)){
            final Integer notifyTime = StringUtils.isNotEmpty(commandLine.getOptionValue(NOTIFY))? Integer.parseInt(commandLine.getOptionValue(NOTIFY)) : DEFAULT_NOTIFY_TIME
            sendNotification(notifyTime,"downgrade",nifiLinkName,bootstrapConf)
        }

        if(activeUrls != null) {
            try {
                disconnectNode(nifiLinkName, bootstrapConf, activeUrls)
            } catch (UnsupportedOperationException uoe) {
                if (isVerbose) {
                    logger.info("Disconnect node operation is not supported: " + uoe.message)
                }
            }
        }

        executeNiFiCommand(nifiLinkName,"stop")
        //Wait before attempting to move/restore files
        Thread.sleep(5000)

        if(!StringUtils.isEmpty(backupNiFiDirName) && Files.exists(Paths.get(backupNiFiDirName)) && Files.isDirectory(Paths.get(backupNiFiDirName))){
            if(isVerbose){
                logger.info("Restoring from Backup Directory {} to {}",backupNiFiDirName,rollbackNiFiDirName)
            }
            restoreFiles(backupNiFiDirName,nifiLinkName,rollbackNiFiDirName)
        }else{
            fileManagerTool.moveRepository(nifiLinkName,rollbackNiFiDirName)
        }

        validNiFiDirectory(rollbackNiFiDirName)
        if(Files.isSymbolicLink(Paths.get(nifiLinkName))) {
            Files.delete(Paths.get(nifiLinkName))
            Files.createSymbolicLink(Paths.get(nifiLinkName), Paths.get(rollbackNiFiDirName))
        }

        executeNiFiCommand(rollbackNiFiDirName,"start")

        if(activeUrls != null) {
            try {
                connectNode(rollbackNiFiDirName, bootstrapConf, activeUrls)
            } catch (UnsupportedOperationException uoe) {
                if (isVerbose) {
                    logger.info("Connect node operation is not supported: " + uoe.message)
                }
            }
        }

        if(isVerbose){
            logger.info("Downgrade completed. Please verify that NiFi is up and running")
        }

    }

    void upgrade(final CommandLine commandLine, final String nifiInstallDirName, final String nifiLinkName, final String bootstrapConf, final ClientFactory clientFactory){

        if(isVerbose){
            logger.info("Starting Upgrade")
        }
        final NiFiProperties nifiProperties = getNiFiProperties(bootstrapConf)
        final Client client = clientFactory.getClient(nifiProperties,nifiLinkName)
        List<String> activeUrls = getActiveUrls(client,nifiProperties)
        String upgradeFile
        String backupNiFiDirName

        if(commandLine.hasOption(UPGRADE_FILE)){
            upgradeFile = commandLine.getOptionValue(UPGRADE_FILE)
        }else{
            throw new ParseException("Missing -u option")
        }

        if(commandLine.hasOption(BACKUP_DIR)){
            backupNiFiDirName = commandLine.getOptionValue(BACKUP_DIR)
        }else{
            backupNiFiDirName = "/tmp/nifi_bak"
        }

        if(commandLine.hasOption(NOTIFY)){
            final Integer notifyTime = StringUtils.isNotEmpty(commandLine.getOptionValue(NOTIFY))? Integer.parseInt(commandLine.getOptionValue(NOTIFY)) : DEFAULT_NOTIFY_TIME
            sendNotification(notifyTime,"upgrade",nifiLinkName,bootstrapConf)
        }

        if(activeUrls != null) {
            try {
                disconnectNode(nifiLinkName, bootstrapConf, activeUrls)
            } catch (UnsupportedOperationException uoe) {
                if (isVerbose) {
                    logger.info("Disconnect node operation is not supported: " + uoe.message)
                }
            }
        }

        executeNiFiCommand(nifiLinkName,"stop")
        backupFiles(backupNiFiDirName,nifiLinkName)
        String upgradeRootDir = install(upgradeFile,nifiLinkName,nifiInstallDirName)
        executeNiFiCommand(upgradeRootDir,"start")

        if(activeUrls != null) {
            try {
                connectNode(upgradeRootDir, bootstrapConf, activeUrls)
            } catch (UnsupportedOperationException uoe) {
                if (isVerbose) {
                    logger.info("Connect node operation is not supported: " + uoe.message)
                }
            }
        }

        if(isVerbose){
            logger.info("Upgrade completed. Please verify that NiFi is up and running")
        }

    }

    void parse(final ClientFactory clientFactory, final String[] args) throws ParseException, IllegalArgumentException {

        final CommandLine commandLine = new DefaultParser().parse(options, args)

        if (commandLine.hasOption(HELP_ARG)) {
            printUsage(null)
        }else if (commandLine.hasOption(TYPE)) {

            if(commandLine.hasOption(VERBOSE_ARG)){
                this.isVerbose = true
            }

            String nifiDirName = null
            String nifiInstallDirName = null
            String nifiLink = null

            if(commandLine.hasOption(NIFI_LINK) && !SystemUtils.IS_OS_WINDOWS){
                nifiLink = commandLine.getOptionValue(NIFI_LINK)
            }else if(commandLine.hasOption(NIFI_LINK) && SystemUtils.IS_OS_WINDOWS){
                throw new IllegalArgumentException("Unable to support symbolic links. Please provide installation path using the -d option")
            }

            if(commandLine.hasOption(NIFI_DIR)){
                nifiDirName = commandLine.getOptionValue(NIFI_DIR)
            }else if(SystemUtils.IS_OS_WINDOWS){
                throw new IllegalArgumentException("Missing -d option")
            }

            if(commandLine.hasOption(NIFI_INSTALL_DIR)){
                nifiInstallDirName = commandLine.getOptionValue(NIFI_INSTALL_DIR)
            }else if(!StringUtils.isEmpty(nifiDirName)){
                nifiInstallDirName = Paths.get(nifiDirName).parent
            }

            if(!SystemUtils.IS_OS_WINDOWS) {

                if (StringUtils.isEmpty(nifiLink) || !Files.isSymbolicLink(Paths.get(nifiLink))) {

                    if (!StringUtils.isEmpty(nifiDirName) && Files.exists(Paths.get(nifiDirName))) {

                        if (StringUtils.isEmpty(nifiLink)) {
                            nifiLink = Paths.get(nifiInstallDirName, "nifi_current").toString()
                        }

                        Files.createSymbolicLink(Paths.get(nifiLink), Paths.get(nifiDirName))

                    } else {
                        throw new IllegalArgumentException("Unable to determine nifi installation path for symbolic link")
                    }

                } else {
                    if (StringUtils.isEmpty(nifiInstallDirName)) {
                        nifiInstallDirName = Paths.get(nifiLink).toRealPath().parent
                    }
                }

            }

            final String nifiCurrentDirName = nifiLink == null ? nifiDirName : nifiLink

            if(!validNiFiDirectory(Paths.get(nifiCurrentDirName).toRealPath().toString())){
                throw new IllegalArgumentException("NiFi Directory is invalid:" + Paths.get(nifiCurrentDirName).toRealPath())
            }

            final String type = commandLine.getOptionValue(TYPE).toLowerCase()
            final String bootstrapConfFileName = Paths.get(nifiCurrentDirName,"conf","bootstrap.conf").toString()

            if(supportedNiFiMinimumVersion(nifiCurrentDirName, SUPPORTED_MINIMUM_VERSION)) {

                if(type.equals("upgrade")){
                    upgrade(commandLine,nifiInstallDirName,nifiCurrentDirName,bootstrapConfFileName,clientFactory)
                }else if(type.equals("downgrade")){
                    downgrade(commandLine,nifiCurrentDirName,bootstrapConfFileName,clientFactory)
                }else{
                    throw new ParseException("Invalid type (-t) value:" + type)
                }

            }else{
                throw new UnsupportedOperationException("Upgrade Tool only supports NiFi versions 1.0.0 and above")
            }

        }else{
            throw new ParseException("Missing -t option")
        }

    }

    public static void main(String[] args) {
        final UpgradeTool tool = new UpgradeTool()
        final ClientFactory clientFactory = new NiFiClientFactory()

        try {
            tool.parse(clientFactory,args)
        } catch (ParseException | RuntimeException e) {
            tool.printUsage(e.getLocalizedMessage());
            System.exit(1)
        }

        System.exit(0)
    }

}

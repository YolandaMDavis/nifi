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

package org.apache.nifi.toolkit.upgrade.filemanager

import com.google.common.collect.Sets
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.DefaultParser
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import org.apache.commons.cli.ParseException
import org.apache.commons.compress.archivers.ArchiveEntry
import org.apache.commons.compress.archivers.ArchiveInputStream
import org.apache.commons.compress.archivers.ArchiveStreamFactory
import org.apache.commons.compress.archivers.tar.TarArchiveEntry
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry
import org.apache.commons.compress.archivers.zip.ZipFile
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.commons.compress.utils.IOUtils
import org.apache.commons.io.FileUtils
import org.apache.commons.io.FilenameUtils
import org.apache.commons.lang3.SystemUtils
import org.apache.nifi.toolkit.upgrade.AbstractUpgradeTool
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.attribute.PosixFilePermission

public class FileManagerTool extends AbstractUpgradeTool{

    private static final String DEFAULT_DESCRIPTION = "This tool is used to perform backup, install and restore activities for a NiFi node. "
    private static final String HELP_ARG = "help"
    private static final String VERBOSE_ARG = "verbose"
    private static final String OPERATION = "operation"
    private static final String NIFI_CURRENT_DIR = "nifiCurrentDir"
    private static final String NIFI_INSTALL_DIR = "nifiInstallDir"
    private static final String NIFI_ROLLBACK_DIR = "nifiRollbackDir"
    private static final String BACKUP_DIR = "backupDir"
    private static final String UPGRADE_FILE = "upgradeFile"
    private static final String MOVE_REPOSITORIES = "moveRepositories"
    private boolean moveRepositories = false
    private final static String SUPPORTED_MINIMUM_VERSION = "1.0.0"
    private static final List<PosixFilePermission> POSIX_PERMISSIONS =
            [PosixFilePermission.OTHERS_EXECUTE,
             PosixFilePermission.OTHERS_WRITE,
             PosixFilePermission.OTHERS_READ,
             PosixFilePermission.GROUP_EXECUTE,
             PosixFilePermission.GROUP_WRITE,
             PosixFilePermission.GROUP_READ,
             PosixFilePermission.OWNER_EXECUTE,
             PosixFilePermission.OWNER_WRITE,
             PosixFilePermission.OWNER_READ];


    FileManagerTool() {
        header = buildHeader(DEFAULT_DESCRIPTION)
        setup()
    }

    FileManagerTool(final String description){
        header = buildHeader(description)
        setup()
    }

    @Override
    protected Logger getLogger() {
        LoggerFactory.getLogger(FileManagerTool.class)
    }

    protected Options getOptions(){
        final Options options = new Options()
        options.addOption(Option.builder("h").longOpt(HELP_ARG).desc("Print help info").build())
        options.addOption(Option.builder("v").longOpt(VERBOSE_ARG).desc("Set mode to verbose (default is false)").build())
        options.addOption(Option.builder("o").longOpt(OPERATION).hasArg().desc("File operation (install | backup | restore)").build())
        options.addOption(Option.builder("c").longOpt(NIFI_CURRENT_DIR).hasArg().desc("Current NiFi Installation Directory (used with install or restore operation)").build())
        options.addOption(Option.builder("d").longOpt(NIFI_INSTALL_DIR).hasArg().desc("NiFi Installation Directory (used with install or restore operation)").build())
        options.addOption(Option.builder("r").longOpt(NIFI_ROLLBACK_DIR).hasArg().desc("NiFi Installation Directory (used with install or restore operation)").build())
        options.addOption(Option.builder("b").longOpt(BACKUP_DIR).hasArg().desc("Backup NiFi Directory").build())
        options.addOption(Option.builder("u").longOpt(UPGRADE_FILE).hasArg().desc("NiFi Upgrade File").build())
        options.addOption(Option.builder("m").longOpt(MOVE_REPOSITORIES).desc("Allow repositories to be moved to upgrade/restored nifi directory (if currently within that directory)").build())
        options
    }

    Set<PosixFilePermission> fromMode(final long mode) {

        Set<PosixFilePermission> permissions = Sets.newHashSet();

        POSIX_PERMISSIONS.eachWithIndex{
            perm,index ->
                if ((mode & (1 << index)) != 0) {
                    permissions.add(perm);
                }
        }

        return permissions;
    }

    Properties getProperties(Path confFileName){
        Properties properties = new Properties()
        File confFile = confFileName.toFile()
        properties.load(new FileInputStream(confFile))
        return properties
    }

    boolean valid(File nifiDir){
        if(nifiDir.isDirectory() && Files.exists(Paths.get(nifiDir.absolutePath,"bin","nifi.sh"))){
            true
        }else {
            false
        }
    }

    void move(final String srcDir, final String oldDir, final String newDir){

        final String oldPathName = srcDir.startsWith("./") ? oldDir + File.separator + srcDir.substring(2,srcDir.length()) : oldDir + File.separator + srcDir
        final String newPathName = srcDir.startsWith("./") ? newDir + File.separator + srcDir.substring(2,srcDir.length()) : newDir + File.separator + srcDir

        final Path oldPath = Paths.get(oldPathName)
        final Path newPath = Paths.get(newPathName)

        if(Files.exists(oldPath)) {
            Files.move(oldPath, newPath)
        }

    }

    void moveRepository(final String dirName, final String installDirName){

        if(isVerbose){
            logger.info("Moving repositories from {} to {}:",dirName,installDirName)
        }

        final String bootstrapConfFileName = dirName + File.separator + "conf" + File.separator + "bootstrap.conf"
        final Properties bootstrapProperties = getProperties(Paths.get(bootstrapConfFileName))
        final String nifiPropertiesFile = getRelativeDirectory(bootstrapProperties.getProperty("conf.dir"),dirName) + File.separator +"nifi.properties"
        final Properties nifiProperties = getProperties(Paths.get(nifiPropertiesFile))
        final String flowFileDirectory = nifiProperties.getProperty("nifi.flowfile.repository.directory")
        final String contentRepositoryDir = nifiProperties.getProperty("nifi.content.repository.directory.default")
        final String provenanceRepositoryDir = nifiProperties.getProperty("nifi.provenance.repository.directory.default")
        final String databaseDirectory = nifiProperties.getProperty("nifi.database.directory")

        if(flowFileDirectory.startsWith("./")){
            if(isVerbose){
                logger.info("Moving flowfile repo")
            }
            move(flowFileDirectory,dirName,installDirName)
        }

        if(contentRepositoryDir.startsWith("./")){
            if(isVerbose){
                logger.info("Moving content repo")
            }
            move(contentRepositoryDir,dirName,installDirName)
        }

        if(provenanceRepositoryDir.startsWith("./")){
            if(isVerbose){
                logger.info("Moving provenance repo")
            }
            move(provenanceRepositoryDir,dirName,installDirName)
        }

        if(databaseDirectory.startsWith("./")){
            if(isVerbose){
                logger.info("Moving database repo")
            }
            move(databaseDirectory,dirName,installDirName)
        }
    }

    void copyState(final String currentNiFiDirName, final String installDirName){

        File stateDir = Paths.get(currentNiFiDirName,"state").toFile()

        if(stateDir.exists()){

            if(Files.exists(Paths.get(installDirName,"state"))){
               Files.delete(Paths.get(installDirName,"state"))
            }

            FileUtils.copyDirectoryToDirectory(stateDir, Paths.get(installDirName).toFile())
        }

    }

    protected void setPosixPermissions(final ArchiveEntry entry, final File outputFile, final ZipFile zipFile){
        int mode = 0

        if (entry instanceof TarArchiveEntry) {
            mode = ((TarArchiveEntry) entry).getMode();

        }else if(entry instanceof ZipArchiveEntry && zipFile != null){
            mode = zipFile.getEntry(entry.name).getUnixMode();
        }

        if(mode == 0){
            mode = outputFile.isDirectory()? TarArchiveEntry.DEFAULT_DIR_MODE: TarArchiveEntry.DEFAULT_FILE_MODE
        }

        Set<PosixFilePermission> permissions = fromMode(mode)
        if(permissions.size() > 0) {
            Files.setPosixFilePermissions(outputFile.toPath(), fromMode(mode));
        }

    }

    void backup(String backupNiFiDirName, String currentNiFiDirName){

        if(isVerbose){
            logger.info("Creating backup in directory:" + backupNiFiDirName)
        }

        File backupNiFiDir = new File(backupNiFiDirName)
        Properties bootstrapProperties =  getProperties(Paths.get(currentNiFiDirName,"conf","bootstrap.conf"))
        File confDir = new File(getRelativeDirectory(bootstrapProperties.getProperty("conf.dir"),currentNiFiDirName))
        File libDir  = new File(getRelativeDirectory(bootstrapProperties.getProperty("lib.dir"),currentNiFiDirName))

        if( backupNiFiDir.exists() && backupNiFiDir.isDirectory()){
            backupNiFiDir.deleteDir()
        }

        backupNiFiDir.mkdirs()

        Files.createDirectory(Paths.get(backupNiFiDirName,"bootstrap_files"))
        FileUtils.copyFileToDirectory(Paths.get(currentNiFiDirName,"conf","bootstrap.conf").toFile(),Paths.get(backupNiFiDirName,"bootstrap_files").toFile())
        FileUtils.copyDirectoryToDirectory(Paths.get(currentNiFiDirName,"lib","bootstrap").toFile(),Paths.get(backupNiFiDirName,"bootstrap_files").toFile())
        Files.createDirectories(Paths.get(backupNiFiDirName,"conf"))
        Files.createDirectories(Paths.get(backupNiFiDirName,"lib"))
        FileUtils.copyDirectoryToDirectory(confDir,Paths.get(backupNiFiDirName).toFile())
        FileUtils.copyDirectoryToDirectory(libDir,Paths.get(backupNiFiDirName).toFile())
        FileUtils.copyDirectoryToDirectory(Paths.get(currentNiFiDirName,"bin").toFile(),new File(backupNiFiDirName))
        FileUtils.copyDirectoryToDirectory(Paths.get(currentNiFiDirName,"docs").toFile(),new File(backupNiFiDirName))
        FileUtils.copyFileToDirectory(Paths.get(currentNiFiDirName,"LICENSE").toFile(),new File(backupNiFiDirName))
        FileUtils.copyFileToDirectory(Paths.get(currentNiFiDirName,"NOTICE").toFile(),new File(backupNiFiDirName))
        FileUtils.copyFileToDirectory(Paths.get(currentNiFiDirName,"README").toFile(),new File(backupNiFiDirName))

        if(isVerbose){
            logger.info("Backup Complete")
        }

    }

    void restore(String backupNiFiDirName, String rollbackNiFiDirName, String currentNiFiDirName){

        if(isVerbose){
            logger.info("Restoring to directory:" + rollbackNiFiDirName)
        }

        File rollbackNiFiDir = new File(rollbackNiFiDirName)
        File rollbackNiFiLibDir = Paths.get(rollbackNiFiDirName,"lib").toFile()
        File rollbackNiFiConfDir = Paths.get(rollbackNiFiDirName,"conf").toFile()
        Properties bootstrapProperties =  getProperties(Paths.get(backupNiFiDirName,"bootstrap_files","bootstrap.conf"))
        File confDir = new File(getRelativeDirectory(bootstrapProperties.getProperty("conf.dir"),rollbackNiFiDirName))
        File libDir  = new File(bootstrapProperties.getProperty("lib.dir"))

        if(!rollbackNiFiDir.isDirectory()){
            rollbackNiFiDir.mkdirs()
        }

        if(!rollbackNiFiLibDir.isDirectory()){
            rollbackNiFiLibDir.mkdirs()
        }

        if(!rollbackNiFiConfDir.isDirectory()){
            rollbackNiFiConfDir.mkdirs()
        }

        if(!libDir.isDirectory()){
            libDir.mkdirs()
        }

        if(!confDir.isDirectory()){
            confDir.mkdirs()
        }

        FileUtils.copyFileToDirectory(Paths.get(backupNiFiDirName,"bootstrap_files","bootstrap.conf").toFile(),Paths.get(rollbackNiFiDirName,"conf").toFile())
        FileUtils.copyDirectoryToDirectory(Paths.get(backupNiFiDirName,"bootstrap_files","bootstrap").toFile(),Paths.get(rollbackNiFiDirName,"lib").toFile())
        FileUtils.copyDirectoryToDirectory(Paths.get(backupNiFiDirName,"bin").toFile(),new File(rollbackNiFiDirName))
        FileUtils.copyDirectoryToDirectory(Paths.get(backupNiFiDirName,"docs").toFile(),new File(rollbackNiFiDirName))
        FileUtils.copyDirectory(Paths.get(backupNiFiDirName,"lib").toFile(),libDir)
        FileUtils.copyDirectory(Paths.get(backupNiFiDirName,"conf").toFile(),confDir)
        FileUtils.copyFileToDirectory(Paths.get(backupNiFiDirName,"LICENSE").toFile(),new File(rollbackNiFiDirName))
        FileUtils.copyFileToDirectory(Paths.get(backupNiFiDirName,"NOTICE").toFile(),new File(rollbackNiFiDirName))
        FileUtils.copyFileToDirectory(Paths.get(backupNiFiDirName,"README").toFile(),new File(rollbackNiFiDirName))

        if(moveRepositories) {
            moveRepository(currentNiFiDirName, rollbackNiFiDirName)
        }

        if(isVerbose){
            logger.info("Restore Completed.")
        }

    }

    String extract(final File upgradeFile, final File installDirName){

        if(isVerbose){
            logger.info("Beginning extraction using {} into installation directory {}",upgradeFile.absolutePath,installDirName.absolutePath)
        }

        String extension = FilenameUtils.getExtension(upgradeFile.getName())
        InputStream fis = extension.equals("gz") ? new GzipCompressorInputStream(new FileInputStream(upgradeFile)) : new FileInputStream(upgradeFile)
        final ArchiveInputStream inputStream = new ArchiveStreamFactory().createArchiveInputStream(new BufferedInputStream(fis))
        final ZipFile zipFile = extension.equals("zip") ? new ZipFile(upgradeFile) : null

        ArchiveEntry entry = inputStream.nextEntry

        if(entry != null){

            String archiveRootDir = null

            while(entry != null){

                if(archiveRootDir == null & entry.name.toLowerCase().startsWith("nifi")){

                    archiveRootDir = entry.name.indexOf(File.separator) > -1 ? entry.name.substring(0, entry.getName().indexOf(File.separator)) : entry.name

                    if(isVerbose){
                        logger.info("Upgrade root directory: {}", archiveRootDir)
                    }

                    File archiveRootDirFile = Paths.get(installDirName.getAbsolutePath(),archiveRootDir).toFile()

                    if(archiveRootDirFile.exists()){
                        archiveRootDirFile.deleteDir()
                    }
                    archiveRootDirFile.mkdirs()
                }

                if(isVerbose){
                    logger.info("Extracting file: {} ",entry.name)
                }

                if(archiveRootDir != null && entry.name.startsWith(archiveRootDir)) {

                    final File outputFile = Paths.get(installDirName.getAbsolutePath(),entry.name).toFile();

                    if (entry.isDirectory()) {

                        if (!outputFile.exists()) {
                            if (!outputFile.mkdirs()) {
                                throw new IllegalStateException(String.format("Couldn't create directory %s.", outputFile.getAbsolutePath()));
                            }
                        }

                    } else {

                        File parentDirectory = outputFile.getParentFile()

                        if(!parentDirectory.exists()){
                            parentDirectory.mkdirs()
                        }

                        final OutputStream outputFileStream = new FileOutputStream(outputFile);
                        IOUtils.copy(inputStream, outputFileStream);
                        outputFileStream.close();
                    }

                    if(!SystemUtils.IS_OS_WINDOWS){
                        setPosixPermissions(entry,outputFile,zipFile)
                    }
                }

                entry = inputStream.nextEntry
            }

            return archiveRootDir

        }else{
            throw new RuntimeException("Attempting to extract upgrade file however it is empty: "+upgradeFile.getName())
        }

    }

    void install(final String upgradeFileName, final String installDirName, final String currentNiFiDirName){

        final File upgradeFile = new File(upgradeFileName)

        if(isVerbose){
            logger.info("Beginning installation into directory:" + installDirName)
        }

        if(upgradeFile.exists()){

            final File installDir = new File(installDirName)

            if(!installDir.exists()){
                installDir.mkdirs()
            }

            final String upgradeRootDirName = extract(upgradeFile,installDir)
            final File upgradeRootDir = Paths.get(installDirName,upgradeRootDirName).toFile()

            if(valid(upgradeRootDir)){

                copyState(currentNiFiDirName,upgradeRootDir.absolutePath)

                if(moveRepositories) {
                    moveRepository(currentNiFiDirName,upgradeRootDir.absolutePath)
                }

            }else{
                throw new RuntimeException("Extract failed: Invalid NiFi Installation. Check the install path provided and retry.")
            }

        }else{
            throw new RuntimeException("Upgrade file provided does not exist")
        }

        if(isVerbose){
            logger.info("Installation Complete")
        }

    }

    void parseInstall(final CommandLine commandLine){

        if(commandLine.hasOption(MOVE_REPOSITORIES)){
            this.moveRepositories = true
        }

        if(!commandLine.hasOption(UPGRADE_FILE)){
            throw new ParseException("Missing -u option")
        } else if(!commandLine.hasOption(NIFI_CURRENT_DIR)){
            throw new ParseException("Missing -c option")
        } else if(!commandLine.hasOption(NIFI_INSTALL_DIR)){
            throw new ParseException("Missing -d option")
        }

        String upgradeFileName = commandLine.getOptionValue(UPGRADE_FILE)
        String nifiCurrentDirName = commandLine.getOptionValue(NIFI_CURRENT_DIR)
        String nifiInstallDirName = commandLine.getOptionValue(NIFI_INSTALL_DIR)

        if(supportedNiFiMinimumVersion(nifiCurrentDirName, SUPPORTED_MINIMUM_VERSION)) {

            if (Files.notExists(Paths.get(upgradeFileName))) {
                throw new ParseException("Missing upgrade file: " + upgradeFileName)
            }

            if (Files.notExists(Paths.get(nifiCurrentDirName))) {
                throw new ParseException("Current NiFi installation link does not exist: " + nifiCurrentDirName)
            }

            install(upgradeFileName, nifiInstallDirName, Paths.get(nifiCurrentDirName).toFile().getCanonicalPath())

        }else{
            throw new UnsupportedOperationException("File Manager Tool only supports NiFi versions 1.0.0 or higher.")
        }

    }

    void parseBackup(final CommandLine commandLine){

        if(!commandLine.hasOption(BACKUP_DIR)){
            throw new ParseException("Missing -b option")
        } else if(!commandLine.hasOption(NIFI_CURRENT_DIR)){
            throw new ParseException("Missing -c option")
        }

        String backupDirName = commandLine.getOptionValue(BACKUP_DIR)
        String nifiCurrentDirName = commandLine.getOptionValue(NIFI_CURRENT_DIR)

        if(supportedNiFiMinimumVersion(nifiCurrentDirName, SUPPORTED_MINIMUM_VERSION)) {

            if (Files.notExists(Paths.get(nifiCurrentDirName))) {
                throw new ParseException("Current NiFi installation link does not exist: " + nifiCurrentDirName)
            }

            backup(backupDirName, Paths.get(nifiCurrentDirName).toFile().getCanonicalPath())

        }else{
            throw new UnsupportedOperationException("File Manager Tool only supports NiFi versions 1.0.0 or higher.")
        }

    }

    void parseRestore(final CommandLine commandLine){

        if(commandLine.hasOption(MOVE_REPOSITORIES)){
            this.moveRepositories = true
        }

        if(!commandLine.hasOption(BACKUP_DIR)) {
            throw new ParseException("Missing -b option")
        }else if(!commandLine.hasOption(NIFI_CURRENT_DIR)){
            throw new ParseException("Missing -c option")
        }else if(!commandLine.hasOption(NIFI_ROLLBACK_DIR)){
            throw new ParseException("Missing -r option")
        }

        String backupDirName = commandLine.getOptionValue(BACKUP_DIR)
        String nifiRollbackDirName = commandLine.getOptionValue(NIFI_ROLLBACK_DIR)
        String nifiCurrentDirName = commandLine.getOptionValue(NIFI_CURRENT_DIR)

        if(supportedNiFiMinimumVersion(nifiCurrentDirName, SUPPORTED_MINIMUM_VERSION)) {

            if (Files.notExists(Paths.get(backupDirName)) || !Files.isDirectory(Paths.get(backupDirName))) {
                throw new ParseException("Missing or invalid backup directory: " + backupDirName)
            }

            if (Files.notExists(Paths.get(nifiCurrentDirName))) {
                throw new ParseException("Current NiFi installation link does not exist: " + nifiCurrentDirName)
            }

            restore(backupDirName, nifiRollbackDirName, Paths.get(nifiCurrentDirName).toFile().getCanonicalPath())

        }else{
            throw new UnsupportedOperationException("File Manager Tool only supports NiFi versions 1.0.0 or higher.")
        }

    }

    void parse(final String[] args) throws ParseException, IllegalArgumentException {

        CommandLine commandLine = new DefaultParser().parse(options, args)

        if (commandLine.hasOption(HELP_ARG)) {
            printUsage(null)
        } else if (commandLine.hasOption(OPERATION)) {

            if(commandLine.hasOption(VERBOSE_ARG)){
                this.isVerbose = true
            }

            String operation = commandLine.getOptionValue(OPERATION).toLowerCase()

            if(operation.equals("install")){
                parseInstall(commandLine)
            }else if(operation.equals("backup")){
                parseBackup(commandLine)
            }else if(operation.equals("restore")){
                parseRestore(commandLine)
            }else{
                throw new ParseException("Invalid operation value:" + operation)
            }

        }else{
            throw new ParseException("Missing -o option")
        }

    }

    public static void main(String[] args) {
        FileManagerTool tool = new FileManagerTool()

        try {
            tool.parse(args)
        } catch (ParseException | IllegalArgumentException e) {
            tool.printUsage(e.getLocalizedMessage());
            System.exit(1)
        }

        System.exit(0)
    }


}

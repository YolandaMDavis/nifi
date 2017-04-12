package org.apache.nifi.toolkit.admin.util

import spock.lang.Specification

class UpgradeUtilSpec  extends Specification{

    def "get nifi version with version in properties"(){

        setup:

        def nifiConfDir = new File("src/test/resources/conf")
        def nifiLibDir = new File("src/test/resources/lib")

        when:

        def version = UpgradeUtil.getNiFiVersion(nifiConfDir,nifiLibDir)

        then:
        version == "1.1.0"
    }

    def "get nifi version with version in nar"(){

        setup:

        def nifiConfDir = new File("src/test/resources/upgrade/conf")
        def nifiLibDir = new File("src/test/resources/lib")

        when:

        def version = UpgradeUtil.getNiFiVersion(nifiConfDir,nifiLibDir)

        then:
        version == "1.2.0"
    }


}

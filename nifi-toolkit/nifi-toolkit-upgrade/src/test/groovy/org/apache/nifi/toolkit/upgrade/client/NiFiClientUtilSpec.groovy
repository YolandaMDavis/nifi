package org.apache.nifi.toolkit.upgrade.client

import com.sun.jersey.api.client.Client
import com.sun.jersey.api.client.ClientResponse
import com.sun.jersey.api.client.WebResource
import org.apache.nifi.util.NiFiProperties
import org.apache.nifi.web.api.entity.ClusterEntity
import org.junit.Rule
import org.junit.contrib.java.lang.system.ExpectedSystemExit
import org.junit.contrib.java.lang.system.SystemOutRule
import spock.lang.Specification

import javax.ws.rs.core.Response

class NiFiClientUtilSpec extends Specification{

    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none()

    @Rule
    public final SystemOutRule systemOutRule = new SystemOutRule().enableLog()

    def "build unsecure url successfully"(){

        given:
        def NiFiProperties niFiProperties = Mock NiFiProperties


        when:
        def url = NiFiClientUtil.getUrl(niFiProperties,"/nifi-api/controller/cluster/nodes/1")

        then:

        3 * niFiProperties.getProperty(_)
        url == "http://localhost:8080/nifi-api/controller/cluster/nodes/1"
    }


    def "get cluster info successfully"(){

        given:
        def Client client = Mock Client
        def NiFiProperties niFiProperties = Mock NiFiProperties
        def WebResource resource = Mock WebResource
        def WebResource.Builder builder = Mock WebResource.Builder
        def ClientResponse response = Mock ClientResponse
        def ClusterEntity clusterEntity = Mock ClusterEntity

        when:
        def entity = NiFiClientUtil.getCluster(client, niFiProperties, [])

        then:

        3 * niFiProperties.getProperty(_)
        1 * client.resource(_ as String) >> resource
        1 * resource.type(_) >> builder
        1 * builder.get(_) >> response
        1 * response.getStatus() >> 200
        1 * response.getEntity(ClusterEntity.class) >> clusterEntity
        entity == clusterEntity

    }

    def "get cluster info fails"(){

        given:
        def Client client = Mock Client
        def NiFiProperties niFiProperties = Mock NiFiProperties
        def WebResource resource = Mock WebResource
        def WebResource.Builder builder = Mock WebResource.Builder
        def ClientResponse response = Mock ClientResponse
        def Response.StatusType statusType = Mock Response.StatusType

        when:

        NiFiClientUtil.getCluster(client, niFiProperties, [])

        then:

        3 * niFiProperties.getProperty(_)
        1 * client.resource(_ as String) >> resource
        1 * resource.type(_) >> builder
        1 * builder.get(_) >> response
        1 * response.getStatus() >> 500
        1 * response.getStatusInfo() >> statusType
        1 * statusType.getReasonPhrase() >> "Only a node connected to a cluster can process the request."
        def e = thrown(RuntimeException)
        e.message == "Unable to obtain cluster information"

    }


}

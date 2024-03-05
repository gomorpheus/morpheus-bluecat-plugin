/*
* Copyright 2022 the original author or authors.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.bluecatnetworks.bluecat

import com.morpheusdata.core.DNSProvider
import com.morpheusdata.core.IPAMProvider
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.util.ConnectionUtils
import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.core.util.NetworkUtility
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.AccountIntegration
import com.morpheusdata.model.Icon
import com.morpheusdata.model.Network
import com.morpheusdata.model.NetworkDomain
import com.morpheusdata.model.NetworkDomainRecord
import com.morpheusdata.model.NetworkPool
import com.morpheusdata.model.NetworkPoolIp
import com.morpheusdata.model.NetworkPoolRange
import com.morpheusdata.model.NetworkPoolServer
import com.morpheusdata.model.NetworkPoolType
import com.morpheusdata.model.OptionType
import com.morpheusdata.model.projection.NetworkDomainIdentityProjection
import com.morpheusdata.model.projection.NetworkDomainRecordIdentityProjection
import com.morpheusdata.model.projection.NetworkPoolIdentityProjection
import com.morpheusdata.response.ServiceResponse
import groovy.util.logging.Slf4j
import io.reactivex.rxjava3.core.Completable
import io.reactivex.rxjava3.schedulers.Schedulers
import org.apache.commons.net.util.SubnetUtils
import io.reactivex.rxjava3.core.Observable
import org.apache.commons.validator.routines.InetAddressValidator

/**
 * The IPAM / DNS Provider implementation for Bluecat
 * This contains most methods used for interacting directly with the Bluecat BAM REST API
 * 
 * @author David Estes
 */
@Slf4j
class BluecatProvider implements IPAMProvider, DNSProvider {

    MorpheusContext morpheusContext
    Plugin plugin
    static String apiVersion = 'v1'

    BluecatProvider(Plugin plugin, MorpheusContext morpheusContext) {
        this.morpheusContext = morpheusContext
        this.plugin = plugin
    }

    /**
     * Creates a manually allocated DNS Record of the specified record type on the passed {@link NetworkDomainRecord} object.
     * This is typically called outside of automation and is a manual method for administration purposes.
     * @param integration The DNS Integration record which contains things like connectivity info to the DNS Provider
     * @param record The domain record that is being requested for creation. All the metadata needed to create teh record
     *               should exist here.
     * @param opts any additional options that may be used in the future to configure behavior. Currently unused
     * @return a ServiceResponse with the success/error state of the create operation as well as the modified record.
     */
    @Override
    ServiceResponse createRecord(AccountIntegration integration, NetworkDomainRecord record, Map opts) {
        ServiceResponse<NetworkDomainRecord> rtn = new ServiceResponse<>()
        HttpApiClient client = new HttpApiClient()
        client.networkProxy = morpheusContext.services.setting.getGlobalNetworkProxy()
        def poolServer = morpheus.network.getPoolServerByAccountIntegration(integration).blockingGet()
        def token
        def rpcConfig
        try {
            if(integration) {
                def fqdn = record.name
                if(!record.name.endsWith(record.networkDomain.name)) {
                    fqdn = "${record.name}.${record.networkDomain.name}"
                }
                def apiPath
                def properties
                def extraProperties

                if(poolServer.configMap?.extraProperties) {
					extraProperties = poolServer.configMap?.extraProperties
				}

                Map<String,String> apiQuery
                rpcConfig = getRpcConfig(poolServer)
                token = login(client,rpcConfig)
                if(token.success) {
                    String apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
                    extraProperties = "${fqdn}|${extraProperties}|".toString()

                    switch(record.type) {
                        case 'CNAME':
                            apiQuery = [absoluteName:fqdn, viewId:record.networkDomain.internalId,linkedRecordName: record.content, ttl:record.ttl?.toString(), type:record.type, properties:extraProperties] as Map<String,String>
                            apiPath = getServicePath(rpcConfig.serviceUrl) + 'addAliasRecord'
                            break
                        default:
                            apiQuery = [absoluteName:fqdn, viewId:record.networkDomain.internalId,rdata: record.content, ttl:record.ttl?.toString(), type:record.type, properties:extraProperties]
                            apiPath = getServicePath(rpcConfig.serviceUrl) + 'addGenericRecord'
                    }

                    def results = client.callJsonApi(apiUrl,apiPath,new HttpApiClient.RequestOptions(queryParams: apiQuery, headers: [Authorization: "BAMAuthToken: ${token.token}".toString()],ignoreSSL: rpcConfig.ignoreSSL),"POST")
                    //error
                    if(results?.success && results?.error != true) {
                        rtn.success = true
                        log.info("Results: ${results.content}")
                        record.externalId = results.content
                        record.name = fqdn
                        return new ServiceResponse<NetworkDomainRecord>(true,null,null,record)
                    } else {
                        log.info("Error: ${results.content}")
                        return ServiceResponse.error(results.content)
                    }
                } else {
                    return ServiceResponse.error("Authentication Error adding DNS Record from Bluecat")
                }

            } else {
                log.warn("no integration")
                return ServiceResponse.error("Integration not found")

            }
        } catch(e) {
            log.error("createRecord error: ${e}", e)
        } finally {
            if(token && token.success) {
                logout(client,rpcConfig,token.token as String)
            }
            client.shutdownClient()
        }
        return rtn
    }

    /**
     * Deletes a Zone Record that is specified on the Morpheus side with the target integration endpoint.
     * This could be any record type within the specified integration and the authoritative zone object should be
     * associated with the {@link NetworkDomainRecord} parameter.
     * @param integration The DNS Integration record which contains things like connectivity info to the DNS Provider
     * @param record The zone record object to be deleted on the target integration.
     * @param opts opts any additional options that may be used in the future to configure behavior. Currently unused
     * @return the ServiceResponse with the success/error of the delete operation.
     */
    @Override
    ServiceResponse deleteRecord(AccountIntegration integration, NetworkDomainRecord record, Map opts) {
        def rtn = new ServiceResponse()
        def rpcConfig
        def token
        try {
            if(integration) {

                morpheus.network.getPoolServerByAccountIntegration(integration).doOnSuccess({ poolServer ->
                    rpcConfig = getRpcConfig(poolServer)
                    HttpApiClient client = new HttpApiClient()
                    client.networkProxy = morpheusContext.services.setting.getGlobalNetworkProxy()
                    try {
                        token = login(client,rpcConfig)
                        if(token.success) {
                            String apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
                            String apiPath = getServicePath(rpcConfig.serviceUrl) + 'delete'
                            HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
                            requestOptions.headers = [Authorization: "BAMAuthToken: ${token.token}".toString()]
                            requestOptions.queryParams = [objectId: record.externalId]
                            //we have an A Record to delete
                            def results = client.callJsonApi(apiUrl, apiPath, null, null, requestOptions, 'DELETE')
                            if (results.success) {
                                rtn.success = true
                            }
                        }
                    } finally {
                        if(token?.success) {
                            logout(client,rpcConfig,token.token as String)
                        }
                        client.shutdownClient()
                    }

                }).doOnError({error ->
                    log.error("Error deleting record: {}",error.message,error)
                }).doOnSubscribe({ sub ->
                    log.debug "Subscribed"
                }).blockingGet()
                return ServiceResponse.success()
            } else {
                log.warn("no integration")
            }
        } catch(e) {
            log.error("provisionServer error: ${e}", e)
        }
        return rtn
    }

    /**
     * Validation Method used to validate all inputs applied to the integration of an IPAM Provider upon save.
     * If an input fails validation or authentication information cannot be verified, Error messages should be returned
     * via a {@link ServiceResponse} object where the key on the error is the field name and the value is the error message.
     * If the error is a generic authentication error or unknown error, a standard message can also be sent back in the response.
     *
     * @param poolServer The Integration Object contains all the saved information regarding configuration of the IPAM Provider.
     * @param opts any custom payload submission options may exist here
     * @return A response is returned depending on if the inputs are valid or not.
     */
    @Override
    ServiceResponse verifyNetworkPoolServer(NetworkPoolServer poolServer, Map opts) {
        ServiceResponse<NetworkPoolServer> rtn = ServiceResponse.error()
        rtn.errors = [:]
        if(!poolServer.name || poolServer.name == ''){
            rtn.errors['name'] = 'name is required'
        }
        if(!poolServer.serviceUrl || poolServer.serviceUrl == ''){
            rtn.errors['serviceUrl'] = 'Bluecat API URL is required'
        }

        if((!poolServer.serviceUsername || poolServer.serviceUsername == '') && (!poolServer.credentialData?.username || poolServer.credentialData?.username == '')){
            rtn.errors['serviceUsername'] = 'username is required'
        }
        if((!poolServer.servicePassword || poolServer.servicePassword == '') && (!poolServer.credentialData?.password || poolServer.credentialData?.password == '')){
            rtn.errors['servicePassword'] = 'password is required'
        }

        rtn.data = poolServer
        if(rtn.errors.size() > 0){
            rtn.success = false
            return rtn //
        }
        def rpcConfig = getRpcConfig(poolServer)
        HttpApiClient bluecatClient = new HttpApiClient()
        def networkProxy = morpheusContext.services.setting.getGlobalNetworkProxy()
        bluecatClient.networkProxy = networkProxy
        def tokenResults
        try {
            def apiUrl = poolServer.serviceUrl
            boolean hostOnline = false
            try {
                def apiUrlObj = new URL(apiUrl)
                def apiHost = apiUrlObj.host
                def apiPort = apiUrlObj.port > 0 ? apiUrlObj.port : (apiUrlObj?.protocol?.toLowerCase() == 'https' ? 443 : 80)
                hostOnline = ConnectionUtils.testHostConnectivity(apiHost, apiPort, false, true, networkProxy)
            } catch(e) {
                log.error("Error parsing URL {}", apiUrl, e)
            }
            if(hostOnline) {
                opts.doPaging = false
                opts.maxResults = 1
                tokenResults = login(bluecatClient,rpcConfig)
                if(tokenResults.success) {
                    def results = listConfigurations(bluecatClient,tokenResults.token as String,poolServer, opts)
                    if(results.success) {
                        rtn.success = true
                    } else {
                        rtn.msg = results.msg ?: 'Error connecting to Bluecat'
                    }
                } else {
                    rtn.msg = tokenResults.msg ?: 'Error authenticating to Bluecat'
                }
            } else {
                rtn.msg = 'Host not reachable'
            }
        } catch(e) {
            log.error("verifyPoolServer error: ${e}", e)
        } finally {
            if(tokenResults?.success) {
                logout(bluecatClient,rpcConfig,tokenResults.token)
            }
            bluecatClient.shutdownClient()
        }
        return rtn
    }

    /**
     * Called during creation of a {@link NetworkPoolServer} operation. This allows for any custom operations that need
     * to be performed outside of the standard operations.
     * @param poolServer The Integration Object contains all the saved information regarding configuration of the IPAM Provider.
     * @param opts any custom payload submission options may exist here
     * @return A response is returned depending on if the operation was a success or not.
     */
    @Override
    ServiceResponse createNetworkPoolServer(NetworkPoolServer poolServer, Map opts) {
        return null
    }

    /**
     * Called during update of an existing {@link NetworkPoolServer}. This allows for any custom operations that need
     * to be performed outside of the standard operations.
     * @param poolServer The Integration Object contains all the saved information regarding configuration of the IPAM Provider.
     * @param opts any custom payload submission options may exist here
     * @return A response is returned depending on if the operation was a success or not.
     */
    @Override
    ServiceResponse updateNetworkPoolServer(NetworkPoolServer poolServer, Map opts) {
        return null
    }

    ServiceResponse testNetworkPoolServer(HttpApiClient client, String token, NetworkPoolServer poolServer) {
        def rtn = new ServiceResponse()
        try {
            def configurationList = listConfigurations(client,token, poolServer, [:])
            rtn.success = configurationList.success
            rtn.data = [:]
            if(!configurationList.success) {
                rtn.msg = 'error connecting to Bluecat'
            }
        } catch(e) {
            rtn.success = false
            log.error("test network pool server error: ${e}", e)
        }
        return rtn
    }

    /**
     * Periodically called to refresh and sync data coming from the relevant integration. Most integration providers
     * provide a method like this that is called periodically (typically 5 - 10 minutes). DNS Sync operates on a 10min
     * cycle by default. Useful for caching Host Records created outside of Morpheus.
     * @param poolServer The Integration Object contains all the saved information regarding configuration of the IPAM Provider.
     */
    @Override
    void refresh(NetworkPoolServer poolServer) {
        log.debug("refreshNetworkPoolServer: {}", poolServer.dump())
        HttpApiClient bluecatClient = new HttpApiClient()
        def networkProxy = morpheusContext.services.setting.getGlobalNetworkProxy()
        bluecatClient.networkProxy = networkProxy
        bluecatClient.throttleRate = poolServer.serviceThrottleRate
        def tokenResults
        def rpcConfig = getRpcConfig(poolServer)
        try {
            def apiUrl = poolServer.serviceUrl
            def apiUrlObj = new URL(apiUrl)
            def apiHost = apiUrlObj.host
            def apiPort = apiUrlObj.port > 0 ? apiUrlObj.port : (apiUrlObj?.protocol?.toLowerCase() == 'https' ? 443 : 80)
            def hostOnline = ConnectionUtils.testHostConnectivity(apiHost, apiPort, false, true, networkProxy)
            log.debug("online: {} - {}", apiHost, hostOnline)
            def testResults
            // Promise

            if(hostOnline) {
                tokenResults = login(bluecatClient,rpcConfig)
                if(tokenResults.success) {
                    testResults = testNetworkPoolServer(bluecatClient,tokenResults.token as String,poolServer) as ServiceResponse<Map>
                    if(!testResults?.success) {
                        morpheus.network.updateNetworkPoolServerStatus(poolServer, AccountIntegration.Status.error, 'error calling Bluecat').subscribe().dispose()
                    } else {
                        morpheus.network.updateNetworkPoolServerStatus(poolServer, AccountIntegration.Status.syncing).subscribe().dispose()
                    }
                } else {
                    morpheus.network.updateNetworkPoolServerStatus(poolServer, AccountIntegration.Status.error, 'error authenticating with Bluecat').subscribe().dispose()
                }
            } else {
                morpheus.network.updateNetworkPoolServerStatus(poolServer, AccountIntegration.Status.error, 'Bluecat api not reachable')
            }
            Date now = new Date()
            if(testResults?.success) {
                String token = tokenResults?.token as String
                cacheNetworks(bluecatClient,token,poolServer)
                cacheZones(bluecatClient,token,poolServer)
                if(poolServer?.configMap?.inventoryExisting) {
                    cacheZoneRecords(bluecatClient,token,poolServer)
                }
                log.info("Sync Completed in ${new Date().time - now.time}ms")
                morpheus.network.updateNetworkPoolServerStatus(poolServer, AccountIntegration.Status.ok).subscribe().dispose()
            }
        } catch(e) {
            log.error("refreshNetworkPoolServer error: ${e}", e)
        } finally {
            if(tokenResults?.success) {
                logout(bluecatClient,rpcConfig,tokenResults.token as String)
            }
            bluecatClient.shutdownClient()
        }
    }

    // cacheNetworks methods
    void cacheNetworks(HttpApiClient client, String token, NetworkPoolServer poolServer, Map opts = [:]) {
        opts.doPaging = true
        def listResults
        if(poolServer.networkFilter?.size() > 0) {
            listResults = collectAllFilteredItems(client,token, poolServer, poolServer.networkFilter.split(',').collect{it.toLong()}, opts)
        } else {
            listResults = collectAllNetworks(client,token,poolServer, opts)
        }
        if(listResults.success) {
            List apiItems = listResults.networks as List<Map>
            Observable<NetworkPoolIdentityProjection> poolRecords = morpheus.network.pool.listIdentityProjections(poolServer.id)

            SyncTask<NetworkPoolIdentityProjection,Map,NetworkPool> syncTask = new SyncTask(poolRecords, apiItems as Collection<Map>)
            syncTask.addMatchFunction { NetworkPoolIdentityProjection domainObject, Map apiItem ->
                domainObject.externalId == "${apiItem.id}"
            }.onDelete {removeItems ->
                morpheus.network.pool.remove(poolServer.id, removeItems).blockingGet()
            }.onAdd { itemsToAdd ->
                addMissingPools(poolServer, itemsToAdd,listResults)
            }.withLoadObjectDetails { List<SyncTask.UpdateItemDto<NetworkPoolIdentityProjection,Map>> updateItems ->
                Map<Long, SyncTask.UpdateItemDto<NetworkPoolIdentityProjection, Map>> updateItemMap = updateItems.collectEntries { [(it.existingItem.id): it]}
                return morpheus.network.pool.listById(updateItems.collect{it.existingItem.id} as Collection<Long>).map { NetworkPool pool ->
                    SyncTask.UpdateItemDto<NetworkPoolIdentityProjection, Map> matchItem = updateItemMap[pool.id]
                    return new SyncTask.UpdateItem<NetworkPool,Map>(existingItem:pool, masterItem:matchItem.masterItem)
                }
            }.onUpdate { List<SyncTask.UpdateItem<NetworkPool,Map>> updateItems ->
                updateMatchedPools(poolServer, updateItems, listResults)
            }.start()
        }
    }

    void addMissingPools(NetworkPoolServer poolServer, Collection<Map> chunkedAddList, listResults) {
        def poolType = new NetworkPoolType(code: 'bluecat')
        def poolTypeIpv6 = new NetworkPoolType(code: 'bluecatipv6')
        List<NetworkPool> missingPoolsList = []
        chunkedAddList?.each { Map network ->
            def networkCidr
            def newNetworkPool
            def networkProps = extractNetworkProperties(network.properties)
            def rangeConfig
            def addRange
            def networkInfo
            if (network.type == 'IP4Network') {
                networkCidr = networkProps['CIDR'] as String
            } else {
                networkCidr = networkProps['prefix'] as String
            }
            def defaultViewId = extractDefaultView(network, networkProps,listResults.networks,listResults.blocks,listResults.views)
            if(networkCidr && network.type == 'IP4Network') {
                networkInfo = getNetworkPoolConfig(networkCidr)

                def addConfig = [account:poolServer.account, poolServer:poolServer, owner:poolServer.account, name:network.name ?: networkCidr, externalId:"${network.id}",
                                 internalId:"${network.configurationId}", cidr: networkCidr, configuration: network.configurationName, type: poolType, poolEnabled:true, parentType:'NetworkPoolServer', parentId:poolServer.id,
                                 dnsSearchPath:defaultViewId]
                addConfig += networkInfo.config
                newNetworkPool = new NetworkPool(addConfig)
                newNetworkPool.ipRanges = []
                networkInfo.ranges?.each { range ->
                    rangeConfig = [startAddress:range.startAddress, endAddress:range.endAddress, addressCount:addConfig.ipCount]
                    addRange = new NetworkPoolRange(rangeConfig)
                    newNetworkPool.ipRanges.add(addRange)
                }
            } else if (networkCidr && network.type == 'IP6Network') {
                def addConfig = [account:poolServer.account, poolServer:poolServer, owner:poolServer.account, name:network.name ?: networkCidr, externalId:"${network.id}",
                                 internalId:"${network.configurationId}", cidr: networkCidr, configuration: network.configurationName, type: poolTypeIpv6, poolEnabled:true, parentType:'NetworkPoolServer', parentId:poolServer.id,
                                 dnsSearchPath:defaultViewId]
                newNetworkPool = new NetworkPool(addConfig)
                newNetworkPool.ipRanges = []
                rangeConfig = [cidrIPv6: networkCidr, startIPv6Address: networkCidr.tokenize('/')[0], endIPv6Address: networkCidr.tokenize('/')[0],addressCount:1]
                addRange = new NetworkPoolRange(rangeConfig)
                newNetworkPool.ipRanges.add(addRange)
            }
            missingPoolsList.add(newNetworkPool)
        }
        morpheus.network.pool.create(poolServer.id, missingPoolsList).blockingGet()
    }

    void updateMatchedPools(NetworkPoolServer poolServer, List<SyncTask.UpdateItem<NetworkPool,Map>> chunkedUpdateList, listResults) {
        List<NetworkPool> poolsToUpdate = []
        chunkedUpdateList?.each { update ->
            NetworkPool existingItem = update.existingItem
            Map network = update.masterItem
            def networkProps = extractNetworkProperties(network.properties)
            def defaultViewId = extractDefaultView(network, networkProps,listResults.networks,listResults.blocks,listResults.views)
            def name
            def networkCidr
            if (network.type == 'IP4Network') {
                name = network.name ?: networkProps['CIDR']
                networkCidr = networkProps['CIDR']
            } else {
                name = network.name ?: networkProps['prefix']
                networkCidr = networkProps['prefix']
            }
            if(existingItem) {
                //update view ?
                Boolean save = false
                if(existingItem.name != name) {
                    existingItem.name = name
                    save = true
                }
                if(defaultViewId != existingItem.dnsSearchPath) {
                    existingItem.dnsSearchPath = defaultViewId
                    save = true
                }
                if(existingItem.cidr != networkCidr) {
                    existingItem.cidr = networkCidr
                    save = true
                }
                if(existingItem.configuration != network.configurationName) {
                    existingItem.configuration = network.configurationName
                    save = true
                }
                if(save) {
                    poolsToUpdate << existingItem
                }
            }
        }
        if(poolsToUpdate.size() > 0) {
            morpheus.network.pool.save(poolsToUpdate).blockingGet()
        }
    }


    // Cache Zones methods
    def cacheZones(HttpApiClient client, String token, NetworkPoolServer poolServer, Map opts = [:]) {
        try {
            def listResults = collectAllZones(client,token,poolServer,opts)
            if (listResults.success) {
                List apiItems = listResults.zones as List<Map>
                Observable<NetworkDomainIdentityProjection> domainRecords = morpheus.network.domain.listIdentityProjections(poolServer.integration.id)

                SyncTask<NetworkDomainIdentityProjection,Map,NetworkDomain> syncTask = new SyncTask(domainRecords, apiItems as Collection<Map>)
                syncTask.addMatchFunction { NetworkDomainIdentityProjection domainObject, Map apiItem ->
                    domainObject.externalId == apiItem.id.toString()
                }.onDelete {removeItems ->
                    morpheus.network.domain.remove(poolServer.integration.id, removeItems).blockingGet()
                }.onAdd { itemsToAdd ->
                    addMissingZones(poolServer, itemsToAdd)
                }.withLoadObjectDetails { List<SyncTask.UpdateItemDto<NetworkDomainIdentityProjection,Map>> updateItems ->
                    Map<Long, SyncTask.UpdateItemDto<NetworkDomainIdentityProjection, Map>> updateItemMap = updateItems.collectEntries { [(it.existingItem.id): it]}
                    return morpheus.network.domain.listById(updateItems.collect{it.existingItem.id} as Collection<Long>).map { NetworkDomain networkDomain ->
                        SyncTask.UpdateItemDto<NetworkDomainIdentityProjection, Map> matchItem = updateItemMap[networkDomain.id]
                        return new SyncTask.UpdateItem<NetworkDomain,Map>(existingItem:networkDomain, masterItem:matchItem.masterItem)
                    }
                }.onUpdate { List<SyncTask.UpdateItem<NetworkDomain,Map>> updateItems ->
                    updateMatchedZones(poolServer, updateItems)
                }.start()
            }
        } catch (e) {
            log.error("cacheZones error: ${e}", e)
        }
    }

    /**
     * Creates a mapping for networkDomainService.createSyncedNetworkDomain() method on the network context.
     * @param poolServer
     * @param addList
     */
    void addMissingZones(NetworkPoolServer poolServer, Collection addList) {
        List<NetworkDomain> missingZonesList = addList?.collect { Map it ->
            def zoneProps = extractNetworkProperties(it.properties)
            String displayName = "${zoneProps.absoluteName ?: it.name} (${it.viewName})"
            def addConfig = [owner:poolServer.account, refType:'AccountIntegration', refId:poolServer.integration.id,
                                                                      externalId:it.id, configuration: it.configurationName, internalId: it.viewId, name: zoneProps.absoluteName ?: it.name, displayName: displayName, fqdn:NetworkUtility.getFqdnDomainName(zoneProps.absoluteName ?: it.name),
                                                                      refSource:'integration', zoneType:'Authoritative']
            NetworkDomain networkDomain = new NetworkDomain(addConfig)
            return networkDomain
        }
        morpheus.network.domain.create(poolServer.integration.id, missingZonesList).blockingGet()
    }

    /**
     * Given a pool server and updateList, extract externalId's and names to match on and update NetworkDomains.
     * @param poolServer
     * @param addList
     */
    void updateMatchedZones(NetworkPoolServer poolServer, List<SyncTask.UpdateItem<NetworkDomain,Map>> updateList) {
        def domainsToUpdate = []
        for(SyncTask.UpdateItem<NetworkDomain,Map> update in updateList) {
            NetworkDomain existingItem = update.existingItem as NetworkDomain
            if(existingItem) {
                Boolean save = false
                if(!existingItem.internalId) {
                    existingItem.internalId = update.masterItem.viewId.toString()
                    save = true
                }

                if(!existingItem.refId) {
                    existingItem.refType = 'AccountIntegration'
                    existingItem.refId = poolServer.integration.id
                    existingItem.refSource = 'integration'
                    save = true
                }

                if(save) {
                    domainsToUpdate.add(existingItem)
                }
            }
        }
        if(domainsToUpdate.size() > 0) {
            morpheus.network.domain.save(domainsToUpdate).blockingGet()
        }
    }

    // Cache Zones methods
    def cacheZoneRecords(HttpApiClient client, String token, NetworkPoolServer poolServer) {
        morpheus.network.domain.listIdentityProjections(poolServer.integration.id).flatMap {NetworkDomainIdentityProjection domain ->
            Completable.mergeArray(
                    cacheZoneDomainRecords(client,token,poolServer, domain),
                    cacheZoneAliasRecords(client,token,poolServer, domain)
            ).toObservable().subscribeOn(Schedulers.io())
        }.doOnError{ e ->
            log.error("cacheZoneRecords error: ${e}", e)
        }.subscribe()
    }
    // Cache Zones methods
    Completable cacheZoneDomainRecords(HttpApiClient client, String token, NetworkPoolServer poolServer, NetworkDomainIdentityProjection domain) {
            def listResults = listZoneRecords(client,token,poolServer,[parentId: domain.externalId])

            if (listResults.success) {
                List<Map> apiItems = listResults.records as List<Map>
                Observable<NetworkDomainRecordIdentityProjection> domainRecords = morpheus.network.domain.record.listIdentityProjections(domain,'A')
                SyncTask<NetworkDomainRecordIdentityProjection, Map, NetworkDomainRecord> syncTask = new SyncTask<NetworkDomainRecordIdentityProjection, Map, NetworkDomainRecord>(domainRecords, apiItems)
                syncTask.addMatchFunction {  NetworkDomainRecordIdentityProjection domainObject, Map apiItem ->
                    domainObject.externalId == apiItem.id?.toString()
                }.onDelete {removeItems ->
                    morpheus.network.domain.record.remove(domain, removeItems).blockingGet()
                }.onAdd { itemsToAdd ->
                    addMissingDomainRecords(domain, itemsToAdd)
                }.withLoadObjectDetails { List<SyncTask.UpdateItemDto<NetworkDomainRecordIdentityProjection,Map>> updateItems ->
                    Map<Long, SyncTask.UpdateItemDto<NetworkDomainRecordIdentityProjection, Map>> updateItemMap = updateItems.collectEntries { [(it.existingItem.id): it]}
                    return morpheus.network.domain.record.listById(updateItems.collect{it.existingItem.id} as Collection<Long>).map { NetworkDomainRecord domainRecord ->
                        SyncTask.UpdateItemDto<NetworkDomainRecordIdentityProjection, Map> matchItem = updateItemMap[domainRecord.id]
                        return new SyncTask.UpdateItem<NetworkDomainRecord,Map>(existingItem:domainRecord, masterItem:matchItem.masterItem)
                    }
                }.onUpdate { List<SyncTask.UpdateItem<NetworkDomainRecord,Map>> updateItems ->
                    updateMatchedDomainRecords(updateItems)
                }
                return Completable.fromObservable(syncTask.observe())
            } else {
                return Completable.complete()
            }
    }

    // Cache Zones methods
    Completable cacheZoneAliasRecords(HttpApiClient client, String token, NetworkPoolServer poolServer, NetworkDomainIdentityProjection domain) {
        def listResults = listZoneCnameRecords(client,token,poolServer,[parentId: domain.externalId])

        if (listResults.success) {
            List<Map> apiItems = listResults.records as List<Map>
            Observable<NetworkDomainRecordIdentityProjection> domainRecords = morpheus.network.domain.record.listIdentityProjections(domain,'CNAME')
            SyncTask<NetworkDomainRecordIdentityProjection, Map, NetworkDomainRecord> syncTask = new SyncTask<NetworkDomainRecordIdentityProjection, Map, NetworkDomainRecord>(domainRecords, apiItems)
            syncTask.addMatchFunction {  NetworkDomainRecordIdentityProjection domainObject, Map apiItem ->
                domainObject.externalId == apiItem.id?.toString()
            }.onDelete {removeItems ->
                morpheus.network.domain.record.remove(domain, removeItems).blockingGet()
            }.onAdd { itemsToAdd ->
                addMissingDomainAliasRecords(domain, itemsToAdd)
            }.withLoadObjectDetails { List<SyncTask.UpdateItemDto<NetworkDomainRecordIdentityProjection,Map>> updateItems ->
                Map<Long, SyncTask.UpdateItemDto<NetworkDomainRecordIdentityProjection, Map>> updateItemMap = updateItems.collectEntries { [(it.existingItem.id): it]}
                return morpheus.network.domain.record.listById(updateItems.collect{it.existingItem.id} as Collection<Long>).map { NetworkDomainRecord domainRecord ->
                    SyncTask.UpdateItemDto<NetworkDomainRecordIdentityProjection, Map> matchItem = updateItemMap[domainRecord.id]
                    return new SyncTask.UpdateItem<NetworkDomainRecord,Map>(existingItem:domainRecord, masterItem:matchItem.masterItem)
                }
            }.onUpdate { List<SyncTask.UpdateItem<NetworkDomainRecord,Map>> updateItems ->
                updateMatchedDomainAliasRecords(updateItems)
            }
            return Completable.fromObservable(syncTask.observe())
        } else {
            return Completable.complete()
        }
    }


    void updateMatchedDomainRecords(List<SyncTask.UpdateItem<NetworkDomainRecord, Map>> updateList) {
        def records = []
        updateList?.each { update ->
            NetworkDomainRecord existingItem = update.existingItem
            def extraProps = extractNetworkProperties(update.masterItem.properties)
            if(existingItem) {
                //update view ?
                def save = false
                if(existingItem.content != extraProps.rdata) {
                    existingItem.setContent(extraProps.rdata)
                    save = true
                }

                if(save) {
                    records.add(existingItem)
                }
            }
        }
        if(records.size() > 0) {
            morpheus.network.domain.record.save(records).blockingGet()
        }
    }

    void addMissingDomainRecords(NetworkDomainIdentityProjection domain, Collection<Map> addList) {
        List<NetworkDomainRecord> records = []

        addList?.each {
            def extraProps = extractNetworkProperties(it.properties)
            def addConfig = [networkDomain: new NetworkDomain(id: domain.id), externalId:it.id, name: extraProps.absoluteName ?: it.name, fqdn:NetworkUtility.getFqdnDomainName(extraProps.absoluteName ?: it.name), type: 'A', source: 'sync']
            def newObj = new NetworkDomainRecord(addConfig)
            newObj.setContent(extraProps.rdata)
            records.add(newObj)
        }
        morpheus.network.domain.record.create(domain,records).blockingGet()
    }


    void addMissingDomainAliasRecords(NetworkDomainIdentityProjection domain, Collection<Map> addList) {
        List<NetworkDomainRecord> records = []

        addList?.each {
            def extraProps = extractNetworkProperties(it.properties)
            def addConfig = [networkDomain: new NetworkDomain(id: domain.id), externalId:it.id, name: extraProps.absoluteName ?: it.name, fqdn:NetworkUtility.getFqdnDomainName(extraProps.absoluteName ?: it.name), type: 'CNAME', source: 'sync']
            def newObj = new NetworkDomainRecord(addConfig)
            newObj.setContent(extraProps.linkedRecordName)
            records.add(newObj)
        }
        morpheus.network.domain.record.create(domain,records).blockingGet()
    }

    void updateMatchedDomainAliasRecords(List<SyncTask.UpdateItem<NetworkDomainRecord, Map>> updateList) {
        def records = []
        updateList?.each { update ->
            NetworkDomainRecord existingItem = update.existingItem
            def extraProps = extractNetworkProperties(update.masterItem.properties)
            if(existingItem) {
                //update view ?
                def save = false
                if(existingItem.content != extraProps.linkedRecordName) {
                    existingItem.setContent(extraProps.linkedRecordName)
                    save = true
                }

                if(save) {
                    records.add(existingItem)
                }
            }
        }
        if(records.size() > 0) {
            morpheus.network.domain.record.save(records).blockingGet()
        }
    }


    /**
     * Called on the first save / update of a pool server integration. Used to do any initialization of a new integration
     * Often times this calls the periodic refresh method directly.
     * @param poolServer The Integration Object contains all the saved information regarding configuration of the IPAM Provider.
     * @param opts an optional map of parameters that could be sent. This may not currently be used and can be assumed blank
     * @return a ServiceResponse containing the success state of the initialization phase
     */
    @Override
    ServiceResponse initializeNetworkPoolServer(NetworkPoolServer poolServer, Map opts) {
        def rtn = new ServiceResponse()
        try {
            if(poolServer) {
                refresh(poolServer)
                rtn.data = poolServer
            } else {
                rtn.error = 'No pool server found'
            }
        } catch(e) {
            rtn.error = "initializeNetworkPoolServer error: ${e}"
            log.error("initializeNetworkPoolServer error: ${e}", e)
        }
        return rtn
    }

    /**
     * Creates a Host record on the target {@link NetworkPool} within the {@link NetworkPoolServer} integration.
     * @param poolServer The Integration Object contains all the saved information regarding configuration of the IPAM Provider.
     * @param networkPool the NetworkPool currently being operated on.
     * @param networkPoolIp The ip address and metadata related to it for allocation. It is important to create functionality such that
     *                      if the ip address property is blank on this record, auto allocation should be performed and this object along with the new
     *                      ip address be returned in the {@link ServiceResponse}
     * @param domain The domain with which we optionally want to create an A/PTR record for during this creation process.
     * @param createARecord configures whether or not the A record is automatically created
     * @param createPtrRecord configures whether or not the PTR record is automatically created
     * @return a ServiceResponse containing the success state of the create host record operation
     */
    @Override
    ServiceResponse createHostRecord(NetworkPoolServer poolServer, NetworkPool networkPool, NetworkPoolIp networkPoolIp, NetworkDomain domain, Boolean createARecord, Boolean createPtrRecord) {
        HttpApiClient client = new HttpApiClient();
        client.networkProxy = morpheusContext.services.setting.getGlobalNetworkProxy()
        InetAddressValidator inetAddressValidator = new InetAddressValidator()
        def rpcConfig = getRpcConfig(poolServer)
        def token

        def extraProperties
        if(poolServer.configMap?.extraProperties) {
            extraProperties = generateExtraProperties(poolServer, networkPoolIp)
        }

        try {
            token = login(client,rpcConfig)
            if(token.success) {
                def hostname = networkPoolIp.hostname
                if(domain && hostname && !hostname.endsWith(domain.name))  {
                    hostname = "${hostname}.${domain.name}"
                }

                def apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
                def apiPath = getServicePath(rpcConfig.serviceUrl) + 'addDeviceInstance'
                def hostInfo
                def viewName = null
                def nextIpv6
                if(networkPool.dnsSearchPath) {
                    def viewobjresults = getEntity(client,token.token as String,poolServer,networkPool.dnsSearchPath.toLong(),[:])
                    if(!viewobjresults.success) {
                        log.warn("Error obtaining default view information for selected pool ${networkPool.name} -- viewId: ${networkPool.dnsSearchPath}")
                    } else {
                        viewName = viewobjresults.entity.name
                    }
                }

                HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
                requestOptions.headers = [Authorization: "BAMAuthToken: ${token.token}".toString()]

                if(!hostname.endsWith('localdomain') && hostname.contains('.') && createARecord != false) {
                    hostInfo = "${hostname},${networkPool.dnsSearchPath ? networkPool.dnsSearchPath : ''},true,false".toString()  //hostname,viewId,reverseFlag,sameAsZoneFlag   
                } else {
                    hostInfo = "${hostname}".toString()  //hostname,viewId,reverseFlag,sameAsZoneFlag
                }
                
                extraProperties = "name=${hostname}|${extraProperties}|".toString()

                requestOptions.queryParams = [parentId:networkPool.externalId, macAddress:'', configurationId:networkPool.internalId, action:'MAKE_STATIC', hostInfo:hostInfo, properties:extraProperties]

                // time to dry without dns
                if(networkPoolIp.ipAddress) {
                    // Make sure it's a valid IP
                    if (inetAddressValidator.isValidInet4Address(networkPoolIp.ipAddress)) {
                        log.debug("A Valid IPv4 Address Entered: ${networkPoolIp.ipAddress}")
                        requestOptions.queryParams.ip4Address = networkPoolIp.ipAddress
                        apiPath = getServicePath(rpcConfig.serviceUrl) + 'assignIP4Address'
                    } else if (inetAddressValidator.isValidInet6Address(networkPoolIp.ipAddress)) {
                        // Check if IPv6 Address Exists
                        log.debug("A Valid IPv6 Address Entered: ${networkPoolIp.ipAddress}")
                        requestOptions.queryParams = [address:networkPoolIp.ipAddress, containerId:networkPool.externalId]
                        apiPath = getServicePath(rpcConfig.serviceUrl) + 'getIP6Address'
                        def results = client.callJsonApi(apiUrl,apiPath,null,null,requestOptions, 'GET')

                        // Add IP6Address to Pool
                        if(results?.success && results?.data?.id != 0) {
                            log.error("IPv6 Address Already in Use: ${networkPoolIp.ipAddress}", results)
                            return ServiceResponse.error("IPv6 Address Already in Use: ${networkPoolIp.ipAddress}")
                        } else {
                            requestOptions.queryParams = [containerId:networkPool.externalId, address:networkPoolIp.ipAddress, name:hostname, type:'IP6Address', properties:extraProperties]
                            apiPath = getServicePath(rpcConfig.serviceUrl) + 'addIP6Address'
                        }

                    } else {
                        log.error("Assign IP Address Error: Invalid IP Address ${networkPoolIp.ipAddress}", results)
                        return ServiceResponse.error("Assign IP Address Error: Invalid IP Address ${networkPoolIp.ipAddress}")
                    }
                } else {
                    log.info("unable to allocate DNS records for bluecat IPAM. Attempting simple ip allocation instead.")
                    
                    if (networkPool.type.code == 'bluecat') {
                        apiPath = getServicePath(rpcConfig.serviceUrl) + 'assignNextAvailableIP4Address'
                    } else if (networkPool.type.code == 'bluecatipv6') {
                        apiPath = getServicePath(rpcConfig.serviceUrl) + 'getNextAvailableIP6Address'
                        requestOptions.queryParams = [parentId:networkPool.externalId]
                        def results = client.callJsonApi(apiUrl,apiPath,null,null,requestOptions, 'GET')
                        if(results?.success && results?.error != true) {
                            nextIpv6 = results.data
                            requestOptions.queryParams = [containerId:networkPool.externalId, address:results.data, name:hostname, type:'IP6Address', properties:extraProperties]
                            apiPath = getServicePath(rpcConfig.serviceUrl) + 'addIP6Address'
                        } else {
                            log.error("Add IPv6 Address to Pool Failed", results)
                            return ServiceResponse.error("Add IPv6 Address to Pool Failed")
                        }
                    }
                }
             
                def results = client.callJsonApi(apiUrl,apiPath,null,null,requestOptions, 'POST')

                // Add IPv6 Host Record
                if(results?.success && networkPool.type.code == 'bluecatipv6' && domain && createARecord != false) {
                    requestOptions.queryParams = [absoluteName:hostname,addresses:networkPoolIp.ipAddress,ttl:'-1',viewId:domain.internalId]
                    apiPath = getServicePath(rpcConfig.serviceUrl) + 'addHostRecord'
                    client.callJsonApi(apiUrl,apiPath,null,null,requestOptions, 'POST')
                }

                if(results?.success && results?.error != true) {
                    log.info("getNextIpAddress: ${results}")
                    if(networkPoolIp.ipAddress) {
                        networkPoolIp.externalId = results.content
                    } else {
                        if (networkPool.type.code == 'bluecat') {
                            def extraProps = extractNetworkProperties(results.data?.properties)
                            networkPoolIp.ipAddress = extraProps.address
                            networkPoolIp.externalId = results.data?.id
                        } else {
                            networkPoolIp.ipAddress = nextIpv6
                            networkPoolIp.externalId = results.data
                        }
                    }
                    if(!hostname.endsWith('localdomain') && hostname.contains('.') && createARecord != false) {
                        networkPoolIp.domain = domain
                    }
                    if (networkPoolIp.id) {
                        networkPoolIp = morpheus.network.pool.poolIp.save(networkPoolIp)?.blockingGet()
                    } else {
                        networkPoolIp = morpheus.network.pool.poolIp.create(networkPoolIp)?.blockingGet()
                    }
                    if(!hostname.endsWith('localdomain') && hostname.contains('.') && createARecord != false) {
                        def domainRecord = new NetworkDomainRecord(networkDomain: domain, networkPoolIp: networkPoolIp, name: hostname, fqdn: hostname, source: 'user', type: 'HOST', externalId: networkPoolIp.externalId)
                        domainRecord.setContent(networkPoolIp.ipAddress)
                        morpheus.network.domain.record.create(domainRecord).blockingGet()
                    }


                    return ServiceResponse.success(networkPoolIp)

                } else {
                    log.info("API Call Failed to allocate IP Address {}",results)
                    return ServiceResponse.error("API Call Failed to allocate IP Address ${results}",null,networkPoolIp)
                }
            } else {
                return ServiceResponse.error("Error acquiring authentication token for Bluecat integration ${poolServer.name} during host record creation.")
            }

        } catch(e) {
            log.error("getNextIpAddress error: ${e}", e)
            return ServiceResponse.error("Unknown Error Processing Create Record in Bluecat ${e.message}",null,networkPoolIp)
        } finally {
            client.shutdownClient()
        }
    }

    /**
     * Updates a Host record on the target {@link NetworkPool} if supported by the Provider. If not supported, send the appropriate
     * {@link ServiceResponse} such that the user is properly informed of the unavailable operation.
     * @param poolServer The Integration Object contains all the saved information regarding configuration of the IPAM Provider.
     * @param networkPool the NetworkPool currently being operated on.
     * @param networkPoolIp the changes to the network pool ip address that would like to be made. Most often this is just the host record name.
     * @return a ServiceResponse containing the success state of the update host record operation
     */
    @Override
    ServiceResponse updateHostRecord(NetworkPoolServer poolServer, NetworkPool networkPool, NetworkPoolIp networkPoolIp) {
        return null
    }

    /**
     * Deletes a host record on the target {@link NetworkPool}. This is used for cleanup or releasing of an ip address on
     * the IPAM Provider.
     * @param networkPool the NetworkPool currently being operated on.
     * @param poolIp the record that is being deleted.
     * @param deleteAssociatedRecords determines if associated records like A/PTR records
     * @return a ServiceResponse containing the success state of the delete operation
     */
    @Override
    ServiceResponse deleteHostRecord(NetworkPool networkPool, NetworkPoolIp poolIp, Boolean deleteAssociatedRecords) {
        HttpApiClient client = new HttpApiClient();
        client.networkProxy = morpheusContext.services.setting.getGlobalNetworkProxy()
        def poolServer = morpheus.network.getPoolServerById(networkPool.poolServer.id).blockingGet()
        def rpcConfig = getRpcConfig(poolServer)
        def token
        try {
            token = login(client,rpcConfig)
            if(token.success) {
                String apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
                String apiPath = getServicePath(rpcConfig.serviceUrl) + 'deleteDeviceInstance'
                HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
                requestOptions.headers = [Authorization: "BAMAuthToken: ${token.token}".toString()]
                requestOptions.queryParams = [identifier:poolIp.ipAddress, configName: poolIp.networkPool.configuration]
                ServiceResponse results = client.callJsonApi(apiUrl,apiPath,null,null,requestOptions,'DELETE')
                if(results.success) {
                    return ServiceResponse.success(poolIp)
                } else if(poolIp.externalId) {
                    log.warn("Error calling deleteDeviceInstance on bluecat, attempting legacy delete if applicable")
                    requestOptions.queryParams = [objectId: poolIp.externalId]
                    apiPath = getServicePath(rpcConfig.serviceUrl) + 'delete'
                    results = client.callJsonApi(apiUrl,apiPath,null,null,requestOptions,'DELETE')
                    //error
                    if(results?.success && results?.error != true) {
                        return ServiceResponse.success(poolIp)
                    } else {
                        if(results.content.contains('Object was not found')) {
                            return ServiceResponse.success(poolIp)
                        }
                    }
                } else {
                    return ServiceResponse.error(results.error ?: 'Error Deleting Host Record', null, poolIp)
                }
            } else {
                return ServiceResponse.error("Error Authenticating with Bluecat",null,poolIp)
            }
        } catch(ex) {
            log.error("Error Deleting Host Record {}",ex.message,ex)
            return ServiceResponse.error("Error Deleting Host Record ${ex.message}",null,poolIp)
        } finally {
            if(token?.success) {
                logout(client,rpcConfig,token.token as String)
            }
            client.shutdownClient()
        }
    }

    def collectAllFilteredItems(HttpApiClient client, String token, NetworkPoolServer poolServer, List<Long> filterList, Map opts = [:]) {
        def rtn = [success:false, configurations:[], blocks:[], networks:[],views:[]]
        try {
            filterList?.each { filterItem ->
                    def entityResults = getEntity(client,token, poolServer, filterItem, opts)
                if(entityResults.success == true) {
                    def entity = entityResults.entity
                    def type = entity.type
                    if(type == 'Configuration') {
                        //if its a configuration - grab all below it
                        opts.configurationId = entity.id
                        opts.configurationName = entity.name
                        opts.parentId = entity.id

                        def viewsList = listViews(client,token, poolServer, opts)

                        if(viewsList.success) {
                            viewsList.views?.each { view ->
                                view.configurationId = configuration.id
                            }
                            rtn.views = viewsList.views //we need this incase its an inherit
                        }
                        def subResults = collectNetworks(client,token, poolServer, opts)
                        if(subResults.success == true) {
                            if(subResults?.blocks?.size() > 0) {
                                rtn.blocks += subResults.blocks
                            }
                            if(subResults?.networks?.size() > 0) {
                                rtn.networks += subResults.networks
                            }
                            rtn.success = true
                        }
                        //add it
                        def configurationMatch = rtn.configurations.find{ it.id == entity.id }
                        if(!configurationMatch)
                            rtn.configurations << entity
                    } else if(type == 'IP4Block') {
                        //if its a block - get parent to get to config
                        def parentResults = getEntityParent(client,token, poolServer, entity.id.toLong(), opts)
                        if(parentResults.success == true) {
                            //add to blocks or configs
                            def block = entity
                            def configuration
                            if(parentResults.parent.type == 'Configuration') {
                                configuration = parentResults.parent
                            } else {
                                configuration = findEntityConfiguration(client,token, poolServer, parentResults.parent.id.toLong(), opts, 1)
                            }
                            if(configuration) {
                                opts.configurationId = configuration.id
                                opts.configurationName = configuration.name
                                opts.parentId = configuration.id
                                def viewsList = listViews(client,token, poolServer, opts)

                                if(viewsList.success) {
                                    viewsList.views?.each { view ->
                                        view.configurationId = configuration.id
                                    }
                                    rtn.views = viewsList.views //we need this incase its an inherit
                                }
                                opts.parentId = block.id

                                def subResults = collectNetworks(client,token, poolServer, opts)
                                if(subResults.success == true) {
                                    if(subResults?.blocks?.size() > 0) {
                                        rtn.blocks += subResults.blocks
                                    }
                                    if(subResults?.networks?.size() > 0) {
                                        rtn.networks += subResults.networks
                                    }
                                    rtn.success = true
                                }
                            }
                            //add block
                            if(block) {
                                def blockMatch = rtn.blocks.find{ it.id == block.id }
                                if(!blockMatch)
                                    rtn.blocks << block
                            }
                            //add configuration
                            if(configuration) {
                                def configurationMatch = rtn.configurations.find{ it.id == configuration.id }
                                if(!configurationMatch)
                                    rtn.configurations << configuration
                            }
                        }
                    } else if(type == 'IP6Block') {
                        //if its a block - get parent to get to config
                        def parentResults = getEntityParent(client,token, poolServer, entity.id.toLong(), opts)
                        if(parentResults.success == true) {
                            //add to blocks or configs
                            def block = entity
                            def configuration
                            if(parentResults.parent.type == 'Configuration') {
                                configuration = parentResults.parent
                            } else {
                                configuration = findEntityConfiguration(client,token, poolServer, parentResults.parent.id.toLong(), opts, 1)
                            }
                            if(configuration) {
                                opts.configurationId = configuration.id
                                opts.configurationName = configuration.name
                                opts.parentId = configuration.id
                                def viewsList = listViews(client,token, poolServer, opts)

                                if(viewsList.success) {
                                    viewsList.views?.each { view ->
                                        view.configurationId = configuration.id
                                    }
                                    rtn.views = viewsList.views //we need this incase its an inherit
                                }
                                opts.parentId = block.id

                                def subResults = collectNetworks(client,token, poolServer, opts)
                                if(subResults.success) {
                                    if(subResults?.blocks?.size() > 0) {
                                        rtn.blocks += subResults.blocks
                                    }
                                    if(subResults?.networks?.size() > 0) {
                                        rtn.networks += subResults.networks
                                    }
                                    rtn.success = true
                                }
                            }
                            //add block
                            if(block) {
                                def blockMatch = rtn.blocks.find{ it.id == block.id }
                                if(!blockMatch)
                                    rtn.blocks << block
                            }
                            //add configuration
                            if(configuration) {
                                def configurationMatch = rtn.configurations.find{ it.id == configuration.id }
                                if(!configurationMatch)
                                    rtn.configurations << configuration
                            }
                        }
                    } else if(type == 'IP4Network') {
                        //if its a network - grab its parent
                        def parentResults = getEntityParent(client,token, poolServer, entity.id.toLong(), opts)
                        if(parentResults.success == true) {
                            //add to blocks or configs
                            def block
                            def configuration
                            if(parentResults.parent.type == 'Configuration') {
                                configuration = parentResults.parent
                            } else if(type == 'IP4Block') {
                                block = parentResults.parent
                                configuration = findEntityConfiguration(client,token, poolServer, block.id.toLong(), opts, 1)
                            } else {
                                configuration = findEntityConfiguration(client,token, poolServer, parentResults.parent.id.toLong(), opts, 1)
                            }
                            //add block and network
                            entity.networkBlockId = block?.id
                            entity.configurationId = configuration?.id
                            entity.configurationName = configuration?.name
                            rtn.networks << entity
                            //add block
                            if(block) {
                                def blockMatch = rtn.blocks.find{ it.id == block.id }
                                if(!blockMatch)
                                    rtn.blocks << block
                            }
                            //add configuration
                            if(configuration) {
                                def configurationMatch = rtn.configurations.find{ it.id == configuration.id }
                                if(!configurationMatch) {
                                    rtn.configurations << configuration
                                }
                                opts.parentId = configuration.id
                                def viewsList = listViews(client,token, poolServer, opts)

                                if(viewsList.success) {
                                    viewsList.views?.each { view ->
                                        view.configurationId = configuration.id
                                    }
                                    rtn.views = viewsList.views //we need this incase its an inherit
                                }
                            }
                            rtn.success = true
                        }
                    } else if(type == 'IP6Network') {
                        //if its a network - grab its parent
                        def parentResults = getEntityParent(client,token, poolServer, entity.id.toLong(), opts)
                        if(parentResults.success == true) {
                            //add to blocks or configs
                            def block
                            def configuration
                            if(parentResults.parent.type == 'Configuration') {
                                configuration = parentResults.parent
                            } else if(type == 'IP6Block') {
                                block = parentResults.parent
                                configuration = findEntityConfiguration(client,token, poolServer, block.id.toLong(), opts, 1)
                            } else {
                                configuration = findEntityConfiguration(client,token, poolServer, parentResults.parent.id.toLong(), opts, 1)
                            }
                            //add block and network
                            entity.networkBlockId = block?.id
                            entity.configurationId = configuration?.id
                            entity.configurationName = configuration?.name
                            rtn.networks << entity
                            //add block
                            if(block) {
                                def blockMatch = rtn.blocks.find{ it.id == block.id }
                                if(!blockMatch)
                                    rtn.blocks << block
                            }
                            //add configuration
                            if(configuration) {
                                def configurationMatch = rtn.configurations.find{ it.id == configuration.id }
                                if(!configurationMatch) {
                                    rtn.configurations << configuration
                                }
                                opts.parentId = configuration.id
                                def viewsList = listViews(client,token, poolServer, opts)

                                if(viewsList.success) {
                                    viewsList.views?.each { view ->
                                        view.configurationId = configuration.id
                                    }
                                    rtn.views = viewsList.views //we need this incase its an inherit
                                }
                            }
                            rtn.success = true
                        }
                    }
                }
            }
        } catch(e) {
            log.error("collect filtered items: ${e}", e)
        }
        log.debug("Collect Filtered Items Results: {}",rtn)
        return rtn
    }

    def findEntityConfiguration(HttpApiClient client, String token, NetworkPoolServer poolServer, Long entityId, Map opts, Integer attempt) {
        def rtn
        attempt = attempt + 1
        def parentResults = getEntityParent(client, token,poolServer, entityId, opts)
        if(parentResults.success) {
            if(parentResults.parent.type == 'Configuration') {
                rtn = parentResults.parent
            } else if(attempt < 100) {
                rtn = findEntityConfiguration(client,token, poolServer, parentResults.parent.id.toLong(), opts, attempt)
            }
        }
        return rtn
    }

    def collectAllNetworks(HttpApiClient client, String token, NetworkPoolServer poolServer, Map opts) {
        def rtn = [success:false, configurations:[], blocks:[], networks:[], views:[]]
        try {
            //list configurations
            def configList = listConfigurations(client, token,poolServer, opts)
            if(configList.success) {
                rtn.configurations = configList.configurations
                //iterate configurations
                def allSuccess = true

                rtn.configurations?.each { configuration ->
                    //list blocks
                    opts.configurationId = configuration.id
                    opts.parentId = configuration.id
                    opts.configurationName = configuration.name
                    def viewsList = listViews(client, token, poolServer, opts)

                    if(viewsList.success) {
                        viewsList.views?.each { view ->
                            view.configurationId = configuration.id
                        }
                        rtn.views = viewsList.views //we need this incase its an inherit
                    }
                    def subResults = collectNetworks(client, token, poolServer, opts)
                    if(subResults.success == true) {
                        if(subResults?.blocks?.size() > 0) {
                            rtn.blocks += subResults.blocks
                        }
                        if(subResults?.networks?.size() > 0) {
                            rtn.networks += subResults.networks
                        }
                    } else {
                        allSuccess = false
                    }
                }
                if(allSuccess) {
                    rtn.success = true
                }
            }


        } catch(e) {
            log.error("collectAllNetworks error: ${e}", e)
        }
        log.debug("Collect All Networks Results: {}",rtn)
        return rtn
    }

    def collectAllZones(HttpApiClient client,String token, NetworkPoolServer poolServer, Map  opts) {
        def rtn = [success:false, configurations:[], zones:[]]
        try {
            //list configurations
            def configList = listConfigurations(client,token,poolServer, opts)
            if(configList.success) {
                rtn.configurations = configList.configurations
                //iterate configurations
                def allSuccess = true

                rtn.configurations?.each { configuration ->
                    //list blocks
                    opts.configurationId = configuration.id
                    opts.parentId = configuration.id
                    opts.configurationName = configuration.name
                    def viewsList = listViews(client,token,poolServer, opts)
                    if(viewsList.success) {
                        viewsList.views?.each { view ->
                            opts.parentId = view.id
                            opts.viewId = view.id
                            opts.viewName = view.name
                            def subResults = collectZones(client,token, poolServer, opts)
                            if(subResults.success) {
                                if(subResults?.zones?.size() > 0) {
                                    rtn.zones += subResults.zones
                                }

                            } else {
                                allSuccess = false
                            }
                        }
                    }
                }
                if(allSuccess) {
                    rtn.success = true
                }
            }


        } catch(e) {
            log.error("collectAllNetworks error: ${e}", e)
        }
        return rtn
    }

    def collectZones(HttpApiClient client, String token, NetworkPoolServer poolServer, Map opts) {
        def rtn = [success:false, zones:[]]
        try {
            //list blocks
            def zonesList = listZones(client,token,poolServer, opts)
            if(zonesList.success) {
                def allSuccess = true
                rtn.zones += zonesList.zones
                zonesList?.zones?.each { zone ->
                    //list networks
                    zone.viewName = opts.viewName
                    zone.viewId = opts.viewId
                    opts.zoneId = zone.id
                    opts.parentId = zone.id
                    def subResults = collectZones(client,token,poolServer, opts)
                    if(subResults.success) {
                        if(subResults?.zones?.size() > 0) {
                            rtn.zones += subResults.zones
                        }

                    } else {
                        allSuccess = false
                    }
                }
                rtn.success = allSuccess
            }
        } catch(e) {
            log.error("collectZones error: ${e}", e)
        }
        return rtn
    }

    def collectNetworks(HttpApiClient client, String token, NetworkPoolServer poolServer, Map opts) {
        def rtn = [success:false, blocks:[], networks:[]]
        try {
            //list blocks
            def networkList = listNetworks(client,token,poolServer, opts)
            if(networkList.success) {
                networkList?.networks?.each { network ->
                    network.networkBlockId = opts.networkBlockId
                    network.configurationId = opts.configurationId
                    network.configurationName = opts.configurationName
                    rtn.networks << network
                }
                def blockList = listNetworkBlocks(client,token,poolServer, opts)
                blockList?.networkBlocks?.each { networkBlock ->
                    rtn.blocks << networkBlock
                }
                def allSuccess = true
                blockList?.networkBlocks?.each { networkBlock ->
                    //list networks
                    opts.networkBlockId = networkBlock.id
                    opts.parentId = networkBlock.id
                    def subResults = collectNetworks(client,token,poolServer, opts)
                    if(subResults.success) {
                        if(subResults?.blocks?.size() > 0) {
                            rtn.blocks += subResults.blocks
                        }
                        if(subResults?.networks?.size() > 0) {
                            rtn.networks += subResults.networks
                        }
                    } else {
                        allSuccess = false
                    }
                }
                rtn.success = allSuccess
            }


        } catch(e) {
            log.error("collectNetworks error: ${e}", e)
        }
        return rtn
    }

    def getEntity(HttpApiClient client, String token, NetworkPoolServer poolServer, Long entityId, Map opts) {
        def rtn = [success:false, entity:null]
        try {
            def rpcConfig = getRpcConfig(poolServer)

            //get the filter list
            def apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
            def apiPath = getServicePath(rpcConfig.serviceUrl) + 'getEntityById'
            HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
            requestOptions.headers = [Authorization: "BAMAuthToken: ${token}".toString()]
            requestOptions.queryParams = [id:entityId.toString()]
            def results = client.callJsonApi(apiUrl,apiPath,null,null,requestOptions,'GET')

            if(results?.success && results?.error != true) {
                rtn.success = true
                rtn.entity = results.data
            }

        } catch(e) {
            log.error("getEntity error: ${e}", e)
        }
        log.debug("get entity results: {}", rtn)
        return rtn
    }

    def getEntityParent(HttpApiClient client, String token, NetworkPoolServer poolServer, Long entityId, Map opts) {
        def rtn = [success:false, parent:null]
        try {
            def rpcConfig = getRpcConfig(poolServer)

            //get the filter list
            def apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
            def apiPath = getServicePath(rpcConfig.serviceUrl) + 'getParent'
            HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
            requestOptions.headers = [Authorization: "BAMAuthToken: ${token}".toString()]
            requestOptions.queryParams = [entityId:entityId.toString()]
            def results = client.callJsonApi(apiUrl,apiPath,null,null,requestOptions,'GET')
            if(results?.success && results?.error != true) {
                rtn.success = true
                rtn.parent = results.data
            }

        } catch(e) {
            log.error("getEntityParent error: ${e}", e)
        }
        log.debug("get parent results: {}", rtn)
        return rtn
    }

    def listConfigurations(HttpApiClient client, String token, NetworkPoolServer poolServer, Map opts) {
        def rtn = [success:false, configurations:[]]
        try {
            def rpcConfig = getRpcConfig(poolServer)

            //get the filter list
            def apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
            def apiPath = getServicePath(rpcConfig.serviceUrl) + 'getEntities'
            def hasMore = true
            def start = 0
            def count = 100
            def attempt = 0
            while(hasMore == true && attempt < 1000) {
                attempt++
                HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
                requestOptions.headers = [Authorization: "BAMAuthToken: ${token}".toString()]
                requestOptions.queryParams = [type:'Configuration', start:start.toString(), count:count.toString(), parentId:"0"]
                def results = client.callJsonApi(apiUrl,apiPath,null,null,requestOptions,'GET')
                if(results?.success && results?.error != true) {
                    rtn.success = true
                }
                if(results?.success && results?.error != true && results.data?.size() > 0) {
                    def rows = results.data
                    rtn.configurations += rows
                    if(results.data?.size() >= count) {
                        start += count
                    } else {
                        hasMore = false
                    }
                } else {
                    hasMore = false
                }
            }

        } catch(e) {
            log.error("List Collections Error: ${e}", e)
        }
        log.debug("List Collections Results: ${rtn}")
        return rtn
    }

    def listNetworkBlocks(HttpApiClient client, String token, NetworkPoolServer poolServer, Map opts) {
        def rtn = [success:false, networkBlocks:[]]
        try {
            def rpcConfig = getRpcConfig(poolServer)
            def apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
            def apiPath = getServicePath(rpcConfig.serviceUrl) + 'getEntities'
            def allBlocks = ['IP4Block','IP6Block']
            for (allBlock in allBlocks) {
                def hasMore = true
                def attempt = 0
                def start = 0
                def count = 100
                while(hasMore == true && attempt < 1000) {
                    attempt++
                    HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
                    requestOptions.headers = [Authorization: "BAMAuthToken: ${token}".toString()]
                    requestOptions.queryParams = [type: allBlock, start:start.toString(), count:count.toString(), parentId:opts.parentId?.toString()]
                    def results = client.callJsonApi(apiUrl,apiPath,null,null,requestOptions,'GET')
                    if(results?.success && results?.error != true && results.data?.size() > 0) {
                        rtn.networkBlocks += results.data
                        if(results.data?.size() >= count) {
                            start += count
                        } else {
                            hasMore = false
                        }
                        rtn.success = true
                    } else {
                        hasMore = false
                    }
                }
            }
        } catch(e) {
            log.error("listNetworkBlocks error: ${e}", e)
        }
        log.debug("List Network Blocks Results: ${rtn}")
        return rtn
    }


    def listViews(HttpApiClient client, String token, NetworkPoolServer poolServer, Map opts) {
        def rtn = [success:false, views:[]]
        try {
            def rpcConfig = getRpcConfig(poolServer)
            def apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
            def apiPath = getServicePath(rpcConfig.serviceUrl) + 'getEntities'
            def hasMore = true
            Integer start = 0i
            Integer count = 100i
            Integer attempt = 0i
            while(hasMore == true && attempt < 1000) {
                attempt++
                HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
                requestOptions.headers = [Authorization: "BAMAuthToken: ${token}".toString()]
                requestOptions.queryParams = [type:'View', start:start.toString(), count:count.toString(), parentId:opts.parentId?.toString()]
                def results = client.callJsonApi(apiUrl,apiPath,null,null,requestOptions,'GET')

                if(results?.success && results?.error != true) {
                    rtn.success = true
                }
                if(results?.success && results?.error != true && results.data?.size() > 0) {
                    rtn.views += results.data
                    if(results.data?.size() >= count) {
                        start += count
                    } else {
                        hasMore = false
                    }
                } else {
                    hasMore = false
                }
            }

        } catch(e) {
            log.error("listViews error: ${e}", e)
        }
        log.debug("List Views Results: ${rtn}")
        return rtn
    }

    def listZoneRecords(HttpApiClient client, String token, NetworkPoolServer poolServer, Map opts) {
        def rtn = [success:false, records:[]]
        try {
            def rpcConfig = getRpcConfig(poolServer)

            String apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
            String apiPath = getServicePath(rpcConfig.serviceUrl) + 'getEntities'
            Boolean hasMore = true
            def start = 0i
            def count = 100i
            def attempt = 0i
            while(hasMore && attempt < 1000) {
                attempt++
                HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
                requestOptions.headers = [Authorization: "BAMAuthToken: ${token}".toString()]
                requestOptions.queryParams = [type:'GenericRecord', start:start.toString(), count:count.toString(), parentId:opts.parentId?.toString()]
                def results = client.callJsonApi(apiUrl,apiPath,null,null,requestOptions,'GET')

                if(results?.success && results?.error != true) {
                    rtn.success = true
                }
                if(results?.success && results?.error != true && results.data?.size() > 0) {
                    rtn.records += results.data
                    if(results.data?.size() >= count) {
                        start += count
                    } else {
                        hasMore = false
                    }
                } else {
                    hasMore = false
                }
            }
        } catch(e) {
            log.error("listZoneRecords error: ${e}", e)
        }
        log.debug("listZoneRecords Results: ${rtn}")
        return rtn
    }


    def listZoneCnameRecords(HttpApiClient client, String token, NetworkPoolServer poolServer, Map opts) {
        def rtn = [success:false, records:[]]
        try {
            def rpcConfig = getRpcConfig(poolServer)

            def apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
            def apiPath = getServicePath(rpcConfig.serviceUrl) + 'getEntities'
            def hasMore = true
            def start = 0
            def count = 100
            def attempt = 0
            while(hasMore && attempt < 1000) {
                attempt++
                HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
                requestOptions.headers = [Authorization: "BAMAuthToken: ${token}".toString()]
                requestOptions.queryParams = [type:'AliasRecord', start:start.toString(), count:count.toString(), parentId:opts.parentId?.toString()]
                def results = client.callJsonApi(apiUrl,apiPath,null,null,requestOptions,'GET')

                if(results?.success && results?.error != true) {
                    rtn.success = true
                }
                if(results?.success && results?.error != true && results.data?.size() > 0) {
                    rtn.records += results.data
                    if(results.data?.size() >= count) {
                        start += count
                    } else {
                        hasMore = false
                    }
                } else {
                    hasMore = false
                }
            }

        } catch(e) {
            log.error("listZoneRecords error: ${e}", e)
        }
        log.debug("listZoneRecords Results: ${rtn}")
        return rtn
    }

    def listZones(HttpApiClient client, String token, NetworkPoolServer poolServer, Map opts) {
        def rtn = [success:false, zones:[]]
        try {
            def rpcConfig = getRpcConfig(poolServer)
            String apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
            String apiPath = getServicePath(rpcConfig.serviceUrl) + 'getEntities'
            Boolean hasMore = true
            Integer start = 0i
            Integer count = 100i
            Integer attempt = 0i
            while(hasMore && attempt < 1000) {
                attempt++
                HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
                requestOptions.headers = [Authorization: "BAMAuthToken: ${token}".toString()]
                requestOptions.queryParams = [type:'Zone', start:start.toString(), count:count.toString(), parentId:opts.parentId?.toString()]
                def results = client.callJsonApi(apiUrl,apiPath,null,null,requestOptions,'GET')

                if(results?.success && results?.error != true) {
                    rtn.success = true
                }
                if(results?.success && results?.error != true && results.data?.size() > 0) {
                    rtn.zones += results.data
                    if(results.data?.size() >= count) {
                        start += count
                    } else {
                        hasMore = false
                    }
                } else {
                    hasMore = false
                }
            }

        } catch(e) {
            log.error("listZones error: ${e}", e)
        }
        log.debug("List Zones Results: ${rtn}")
        return rtn
    }

    def listNetworks(HttpApiClient client, String token, NetworkPoolServer poolServer, Map opts) {
        def rtn = [success:false, error:false, networks:[]]
        try {
            def rpcConfig = getRpcConfig(poolServer)
            def apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
            def apiPath = getServicePath(rpcConfig.serviceUrl) + 'getEntities'         
            def doPaging = opts.doPaging != null ? opts.doPaging : true
            def allNetworks = ['IP4Network','IP6Network']
            for (allNetwork in allNetworks) {
                def hasMore = true
                def attempt = 0
                def start = 0
                def count = opts.maxResults != null ? opts.maxResults : 100
                while(hasMore && attempt < 1000) {
                    attempt++
                    HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
                    requestOptions.headers = [Authorization: "BAMAuthToken: ${token}".toString()]
                    requestOptions.queryParams = [type: allNetwork, start:start.toString(), count:count.toString()]
                    if(opts.parentId) {
                        requestOptions.queryParams.parentId = opts.parentId.toString()
                    }

                    def results = client.callJsonApi(apiUrl,apiPath,null,null,requestOptions,'GET')

                    if(results?.success && results?.error != true) {
                        rtn.success = true
                        if(results.data?.size() > 0) {
                            rtn.networks += results.data
                            if(doPaging == true && results.data?.size() >= count) {
                                start += count
                            } else {
                                hasMore = false
                            }
                        } else {
                            //no more content
                            hasMore = false
                        }
                    } else {
                        //error
                        hasMore = false
                        //check for bad creds
                        if(results.errorCode == 401i) {
                            rtn.errorCode = 401i
                            rtn.invalidLogin = true
                            rtn.success = true
                            rtn.error = true
                            rtn.msg = results.content ?: 'invalid credentials'
                        } else if(results.errorCode == 400i) {
                            //request
                            rtn.errorCode = 400i
                            //consider this success - just no content
                            rtn.success = true
                            rtn.error = false
                            rtn.msg = results.content ?: 'invalid api request'
                        } else {
                            rtn.errorCode = results.errorCode ?: 500i
                            rtn.success = false
                            rtn.error = true
                            rtn.msg = results.content ?: 'unknown api error'
                            log.warn("error: ${rtn.errorCode} - ${rtn.content}")
                        }
                    }
                }
            }

        } catch(e) {
            log.error("listNetworks error: ${e}", e)
        }
        log.debug("List Networks Results: ${rtn}")
        return rtn
    }

    /**
     * Periodically called to refresh and sync data coming from the relevant integration. Most integration providers
     * provide a method like this that is called periodically (typically 5 - 10 minutes). DNS Sync operates on a 10min
     * cycle by default. Useful for caching DNS Records created outside of Morpheus.
     * NOTE: This method is unused when paired with a DNS Provider so simply return null
     * @param integration The Integration Object contains all the saved information regarding configuration of the DNS Provider.
     */
    @Override
    void refresh(AccountIntegration integration) {
        //NOOP
    }

    /**
     * Validation Method used to validate all inputs applied to the integration of an DNS Provider upon save.
     * If an input fails validation or authentication information cannot be verified, Error messages should be returned
     * via a {@link ServiceResponse} object where the key on the error is the field name and the value is the error message.
     * If the error is a generic authentication error or unknown error, a standard message can also be sent back in the response.
     * NOTE: This is unused when paired with an IPAMProvider interface
     * @param integration The Integration Object contains all the saved information regarding configuration of the DNS Provider.
     * @param opts any custom payload submission options may exist here
     * @return A response is returned depending on if the inputs are valid or not.
     */
    @Override
    ServiceResponse verifyAccountIntegration(AccountIntegration integration, Map opts) {
        //NOOP
        return null
    }

    /**
     * An IPAM Provider can register pool types for display and capability information when syncing IPAM Pools
     * @return a List of {@link NetworkPoolType} to be loaded into the Morpheus database.
     */
    @Override
    Collection<NetworkPoolType> getNetworkPoolTypes() {
        return [
                new NetworkPoolType(code:'bluecat', name:'Bluecat', creatable:false, description:'Bluecat', rangeSupportsCidr: false, hostRecordEditable: false),
                new NetworkPoolType(code:'bluecatipv6', name:'Bluecat IPv6', creatable:false, description:'Bluecat IPv6', rangeSupportsCidr: true, hostRecordEditable: false, ipv6Pool:true)
        ]
    }

    /**
     * Provide custom configuration options when creating a new {@link AccountIntegration}
     * @return a List of OptionType
     */
    @Override
    List<OptionType> getIntegrationOptionTypes() {
        return [
                new OptionType(code: 'bluecat.serviceUrl', name: 'Service URL', inputType: OptionType.InputType.TEXT, fieldName: 'serviceUrl', fieldLabel: 'API Url', fieldContext: 'domain', helpBlock: 'Warning! Using HTTP URLS are insecure and not recommended.', required:true, displayOrder: 0),
                new OptionType(code: 'bluecat.credentials', name: 'Credentials', inputType: OptionType.InputType.CREDENTIAL, fieldName: 'type', fieldLabel: 'Credentials', fieldContext: 'credential', required: true, displayOrder: 1, defaultValue: 'local',optionSource: 'credentials',config: '{"credentialTypes":["username-password"]}'),
                new OptionType(code: 'bluecat.serviceUsername', name: 'Service Username', inputType: OptionType.InputType.TEXT, fieldName: 'serviceUsername', fieldLabel: 'Username', fieldContext: 'domain', displayOrder: 2, localCredential: true, required:true),
                new OptionType(code: 'bluecat.servicePassword', name: 'Service Password', inputType: OptionType.InputType.PASSWORD, fieldName: 'servicePassword', fieldLabel: 'Password', fieldContext: 'domain', displayOrder: 3, localCredential: true, required:true),
                new OptionType(code: 'bluecat.throttleRate', name: 'Throttle Rate', inputType: OptionType.InputType.NUMBER, defaultValue: 0, fieldName: 'serviceThrottleRate', fieldLabel: 'Throttle Rate', fieldContext: 'domain', displayOrder: 4),
                new OptionType(code: 'bluecat.ignoreSsl', name: 'Ignore SSL', inputType: OptionType.InputType.CHECKBOX, defaultValue: 0, fieldName: 'ignoreSsl', fieldLabel: 'Disable SSL SNI Verification', fieldContext: 'domain', displayOrder: 5),
                new OptionType(code: 'bluecat.inventoryExisting', name: 'Inventory Existing', inputType: OptionType.InputType.CHECKBOX, defaultValue: 0, fieldName: 'inventoryExisting', fieldLabel: 'Inventory Existing', fieldContext: 'config', displayOrder: 6),
                new OptionType(code: 'bluecat.networkFilter', name: 'Network Filter', inputType: OptionType.InputType.TEXT, fieldName: 'networkFilter', fieldLabel: 'Network Filter', fieldContext: 'domain', displayOrder: 7),
                new OptionType(code: 'bluecat.extraProperties', name: 'Extra Properties', inputType: OptionType.InputType.TEXT, fieldName: 'extraProperties', fieldLabel: 'Extra Properties', fieldContext: 'config', displayOrder: 8, helpText: "key=value|key2=value2")
        ]
    }

    /**
     * Returns the IPAM Integration logo for display when a user needs to view or add this integration
     * @since 0.12.3
     * @return Icon representation of assets stored in the src/assets of the project.
     */
    @Override
    Icon getIcon() {
        return new Icon(path:"bluecat.svg", darkPath: "bluecat-dark.svg")
    }

    /**
     * Returns the Morpheus Context for interacting with data stored in the Main Morpheus Application
     *
     * @return an implementation of the MorpheusContext for running Future based rxJava queries
     */
    @Override
    MorpheusContext getMorpheus() {
        return morpheusContext
    }

    /**
     * Returns the instance of the Plugin class that this provider is loaded from
     * @return Plugin class contains references to other providers
     */
    @Override
    Plugin getPlugin() {
        return plugin
    }

    /**
     * A unique shortcode used for referencing the provided provider. Make sure this is going to be unique as any data
     * that is seeded or generated related to this provider will reference it by this code.
     * @return short code string that should be unique across all other plugin implementations.
     */
    @Override
    String getCode() {
        return "bluecat"
    }

    /**
     * Provides the provider name for reference when adding to the Morpheus Orchestrator
     * NOTE: This may be useful to set as an i18n key for UI reference and localization support.
     *
     * @return either an English name of a Provider or an i18n based key that can be scanned for in a properties file.
     */
    @Override
    String getName() {
        return "Bluecat"
    }

    def login(HttpApiClient client, rpcConfig) {
        def rtn = [success:false]
        try {
            def apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
            def apiPath = getServicePath(rpcConfig.serviceUrl) + 'login'
            Map<String,String> apiQuery = [username:rpcConfig.username, password:rpcConfig.password]
            def results = client.callJsonApi(apiUrl,apiPath,new HttpApiClient.RequestOptions(queryParams: apiQuery, ignoreSSL: rpcConfig.ignoreSSL),"GET")

            if(results?.success && results?.error != true) {
                log.debug("login: ${results}")
                def rawToken = results.content
                def startIndex = rawToken.indexOf('BAMAuthToken')
                def endIndex = rawToken.indexOf('<-')
                if(startIndex > -1 && endIndex > -1) {
                    rtn.token = rawToken.substring(startIndex + 13, endIndex).trim()
                    rtn.success = true
                }
            } else {
                //error
            }
        } catch(e) {
            log.error("getToken error: ${e}", e)
        }
        return rtn
    }

    void logout(HttpApiClient client, rpcConfig, String token) {
        try {
            def apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
            def apiPath = getServicePath(rpcConfig.serviceUrl) + 'logout'
            HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
            requestOptions.headers = [Authorization: "BAMAuthToken: ${token}".toString()]
            client.callJsonApi(apiUrl,apiPath,requestOptions,"GET")
        } catch(e) {
            log.error("logout error: ${e}", e)
        }
    }


    private String cleanServiceUrl(String url) {
        String rtn = url
        def slashIndex = rtn.indexOf('/', 10)
        if(slashIndex > 10)
            rtn = rtn.substring(0, slashIndex)
        return rtn
    }

    private String getServicePath(String url) {
        String rtn = '/'
        def slashIndex = url.indexOf('/', 10)
        if(slashIndex > 10)
            rtn = url.substring(slashIndex)
        if(!rtn.endsWith('/'))
            rtn = rtn + '/'
        if(rtn?.length() < 2)
            rtn = '/Services/REST/' + apiVersion + '/'
        return rtn
    }

    private getRpcConfig(NetworkPoolServer poolServer) {
        return [
                username:poolServer.credentialData?.username ?: poolServer.serviceUsername,
                password:poolServer.credentialData?.password ?: poolServer.servicePassword,
                serviceUrl:poolServer.serviceUrl,
                ignoreSSL: poolServer.ignoreSsl
        ]
    }

    private extractNetworkProperties(props) {
        def rtn = [:]
        if(props) {
            def propList = props.tokenize('|')
            propList.each { prop ->
                def pair = prop.tokenize('=')
                if(pair?.size() > 1)
                    rtn[pair[0]] = pair[1]
            }
        }
        return rtn
    }

    String extractDefaultView(Map network, Map networkProps, Collection networks, Collection blocks,Collection views) {
        if(networkProps.defaultView) {
            return networkProps.defaultView.toString()
        } else if(networkProps.inheritDefaultView == "true") {
            //walk the parent
            if(network.networkBlockId) {
                def block = blocks.find{it.id == network.networkBlockId}
                if(block) {
                    def blockProperties = extractNetworkProperties(block.properties)
                    if(blockProperties.defaultView) {
                        return blockProperties.defaultView.toString()
                    } else if(blockProperties.inheritDefaultView == "true") {
                        def view = views.find{view -> view.configurationId == network.configurationId}
                        if(view) {
                            return view.id.toString()
                        }
                    }
                }
            }
        }
        return null
    }

    static formatDate(Object date, String outputFormat = "yyyy-MM-dd' 'HH:mm:ss.SSSSSSSSS") {
        def rtn
        try {
            if(date) {
                if(date instanceof Date)
                    rtn = date.format(outputFormat, TimeZone.getTimeZone('GMT'))
                else if(date instanceof CharSequence)
                    rtn = date
            }
        } catch(ignored) { }
        return rtn
    }

    private String generateExtraProperties(NetworkPoolServer poolServer, NetworkPoolIp networkPoolIp) {
		try {
			def extraProperties = poolServer.configMap?.extraProperties
            if (extraProperties.contains('<%=username%>')) {
                extraProperties = extraProperties.replaceAll('<%=username%>', networkPoolIp.createdBy?.username)
            }
            if (extraProperties.contains('<%=userId%>')) {
                extraProperties = extraProperties.replaceAll('<%=userId%>', networkPoolIp.createdBy?.id)
            }
            if (extraProperties.contains('<%=dateCreated%>')) {
                extraProperties = extraProperties.replaceAll('<%=dateCreated%>', formatDate(new Date()))
            }
			
			log.debug("Extra Properties for Bluecat: {}", extraProperties)
			return extraProperties
		} catch(ex) {
			log.error("Error generating extra properties for Bluecat: {}",ex.message,ex)
			return null
		}

	}

    static Map getNetworkPoolConfig(String cidr) {
        def rtn = [config:[:], ranges:[]]
        try {
            def subnetInfo = new SubnetUtils(cidr).getInfo()
            rtn.config.netmask = subnetInfo.getNetmask()
            rtn.config.ipCount = subnetInfo.getAddressCountLong() ?: 0
            rtn.config.ipFreeCount = rtn.config.ipCount
            rtn.ranges << [startAddress:subnetInfo.getLowAddress(), endAddress:subnetInfo.getHighAddress()]
        } catch(e) {
            log.warn("error parsing network pool cidr: ${e}", e)
        }
        return rtn
    }

}

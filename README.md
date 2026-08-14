# Morpheus BlueCat Plugin

The Morpheus BlueCat Plugin integrates Morpheus with BlueCat Address Manager (BAM) to provide IP address management (IPAM) and DNS record automation. The plugin communicates with the BlueCat REST API to allocate IP addresses, register DNS records, and synchronise network pools.

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Repository structure](#repository-structure)
- [Building the plugin](#building-the-plugin)
- [License](#license)
- [Installing](#installing)
- [Detailed Usage Steps](#detailed-usage-steps)
- [API Endpoints](#api-endpoints)

---

## Features

### IP Address Management

Allocate and release IP addresses from BlueCat Address Manager network pools within Morpheus. Supports automatic next-available IP selection, manual IP entry, and existing inventory import.

### DNS Record Management

Create and delete A, AAAA, CNAME, TXT, and MX DNS records in BlueCat zones when instances are provisioned or decommissioned. Supports optional Quick Deploy to push changes immediately to DNS servers.

### Cloud Sync

Morpheus synchronises the following BlueCat resources for inventory:

- Network pools (subnets managed in BAM)
- DNS zones and records

---

## Requirements

| Requirement | Version |
|-------------|---------|
| Morpheus | 9.0.0 or later |
| Java | 25 or later |
| Gradle | Use the included Gradle wrapper (`./gradlew`) |

Additional prerequisites:

- A running BlueCat Address Manager instance accessible over HTTP or HTTPS
- A BlueCat user account with API access and sufficient permissions to read/write networks and DNS records
- Network access from the Morpheus appliance to the BAM API host over the configured port
- HTTPS recommended; HTTP is supported but not recommended (configure per-integration)

---

## Repository structure

```
src/main/groovy/com/bluecatnetworks/bluecat/
├── BluecatPlugin.groovy    - Plugin entry point; registers BluecatProvider
└── BluecatProvider.groovy  - IPAMProvider implementation; IPAM and DNS operations, sync, OptionTypes
build.gradle, gradle.properties - Build configuration and plugin metadata
```

---

## Building the plugin

Run the following command to compile and package the plugin jar:

```bash
./gradlew clean build
```

The packaged jar will be written to `build/libs/`.

To execute tests, use the following command:

```bash
./gradlew test
```

---

## License

This project is licensed under the Apache License 2.0.

See the [LICENSE](LICENSE) file for details.

---

## Installing

1. Build the plugin (see [Building the plugin](#building-the-plugin)) or download a released jar.
2. In Morpheus, navigate to **Administration > Integrations > Plugins**.
3. Click **Add** and upload the `morpheus-bluecat-plugin-<version>.jar` from `build/libs/`.
4. Navigate to **Infrastructure > Networks > IP Pools > Add** and select **BlueCat** to configure the integration.

---

## Detailed Usage Steps

### Adding a BlueCat IPAM Integration

1. Go to **Infrastructure > Networks > IP Pools > Add**.
2. Select **BlueCat** as the pool server type.
3. Enter the **API Url** (e.g. `https://bam.example.com`), **Username**, and **Password** (or select a stored credential).
4. Optionally configure **Throttle Rate**, **Disable SSL SNI Verification**, **Inventory Existing**, **Run Quick Deploy**, **Network Filter**, and **Extra Properties**.
5. Save. Morpheus connects to BAM and syncs available network pools.

### Allocating an IP Address

When provisioning an instance on a network backed by a BlueCat pool, Morpheus automatically calls BAM to reserve the next available IP. The reserved IP is assigned to the instance and a DNS record is created if DNS is configured on the network.

### Releasing an IP Address

When an instance is decommissioned, Morpheus calls BAM to release the IP and delete the associated DNS records.

---

## API Endpoints

This plugin communicates with the **BlueCat Address Manager REST API** at the configured service URL. Authentication uses a session token obtained at login. All calls use HTTP or HTTPS as configured.

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `<serviceUrl>/Services/REST/v1/login` | GET | Obtain BAM session token |
| `<serviceUrl>/Services/REST/v1/addAliasRecord` | POST | Create CNAME record |
| `<serviceUrl>/Services/REST/v1/addGenericRecord` | POST | Create A/AAAA/TXT/MX record |
| `<serviceUrl>/Services/REST/v1/delete` | DELETE | Delete a DNS record |
| `<serviceUrl>/Services/REST/v1/getNextAvailableIP4Address` | GET | Allocate next available IPv4 address |
| `<serviceUrl>/Services/REST/v1/assignIP4Address` | POST | Register an allocated IP |
| `<serviceUrl>/Services/REST/v1/unassignIP4Address` | DELETE | Release an IP address |
| `<serviceUrl>/Services/REST/v1/getEntitiesByType` | GET | List networks and pools |

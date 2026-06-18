# Morpheus Bluecat Plugin

This plugin provides an IPAM and DNS integration between [BlueCat Address Manager](https://bluecatnetworks.com/) and [Morpheus](https://morpheusdata.com). It enables IPv4 and IPv6 network pool sync, DNS zone and record inventory, host record management, IP allocation, and IP release automation from within the Morpheus platform.

## Requirements

| Component | Minimum Version |
|-----------|----------------|
| Morpheus | 9.0.0 |

## Installation

1. Download the latest `.jar` from the [Releases](https://github.com/HewlettPackard/morpheus-bluecat-plugin/releases) page, or [build it yourself](#building).
2. In Morpheus, navigate to **Administration → Integrations → Plugins**.
3. Click **Browse** and upload the `.jar` file.
4. The **Bluecat** IPAM/DNS network service integration will appear after the plugin loads.

## Configuration

When adding a Bluecat network service in Morpheus (**Infrastructure → Network → Services**), provide the following:

| Field | Description |
|-------|-------------|
| **API Url** | BlueCat Address Manager API endpoint root URL. |
| **Credentials** | Morpheus credential containing the BlueCat username and password. |
| **Username** | BlueCat username used when local credentials are selected. |
| **Password** | BlueCat password used when local credentials are selected. |
| **Throttle Rate** | Optional API throttle rate for BlueCat requests. |
| **Disable SSL SNI Verification** | Disables SSL SNI verification when connecting to BlueCat. |
| **Inventory Existing** | Syncs existing DNS A and CNAME records from BlueCat into Morpheus. |
| **Run Quick Deploy** | Triggers BlueCat Quick Deploy after network and DNS changes. |
| **Network Filter** | Optional comma-separated list of BlueCat entity IDs used to limit synced networks. |
| **Extra Properties** | Additional BlueCat properties sent as `key=value|key2=value2`. |

Credentials can also be stored as a Morpheus [Credential](https://docs.morpheusdata.com/en/latest/administration/credentials/credentials.html) and selected at network service setup time.

## Features

### IPAM Sync

The plugin implements `IPAMProvider` and keeps Morpheus network pools aligned with BlueCat Address Manager.

- **IPv4 networks** — synced as Bluecat network pools with CIDR and range data
- **IPv6 networks** — synced as Bluecat IPv6 network pools
- **Configurations and DNS views** — associated with synced pools for downstream allocation and record creation
- **Filtered sync** — optionally limits inventory to selected BlueCat configurations, blocks, or networks

Any additions, updates, and removals in BlueCat are automatically reflected in Morpheus on the next network service refresh.

### IP Allocation and Release

Morpheus can allocate and release addresses from synced BlueCat pools during workload lifecycle operations. Supported operations include:

- Assign a requested IPv4 or IPv6 address when available
- Allocate the next available IPv4 or IPv6 address from a pool
- Create host records during allocation
- Release IP/device records when workloads are removed
- Optionally run BlueCat Quick Deploy after allocation or release changes

### DNS Zone Sync

The plugin implements `DNSProvider` and discovers authoritative DNS zones from BlueCat.

- **Authoritative zones** — synced into Morpheus as network domains
- **DNS views** — retained on synced zones for record placement
- **Existing record inventory** — optional sync of existing records when enabled in configuration

### DNS Record Management

DNS records can be managed from Morpheus through the BlueCat API. Supported operations include:

- Create generic DNS records
- Create CNAME alias records
- Delete DNS records
- Sync existing A records
- Sync existing CNAME records

## Building

```bash
./gradlew shadowJar
```

The plugin JAR will be written to `build/libs/`.

## License

Copyright 2024 Morpheus Data, LLC. Licensed under the [Apache License, Version 2.0](LICENSE).

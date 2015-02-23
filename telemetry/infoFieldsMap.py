#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# Mappings of new unified-ping fields to classic-ping "info" fields

# Maps unified-ping.application.* => classic-ping.info.*
appFieldMap = {
    "appName": ["name"],
    "appUpdateChannel": ["channel"],
    "appVersion": ["version"],
    "appBuildID": ["buildId"],
    "arch": ["architecture"],
    # don't think these were in the classic desktop ping
    "platformVersion": ["platformVersion"],
    "xpcomAbi": ["xpcomAbi"],
    "vendor": ["vendor"],
}

# Maps unified-ping.environment.* => classic-ping.info.*
envFieldMap = {
    # environment.build section
    "appID": ["build", "applicationId"],
    "platformBuildID": ["build", "buildId"],
    # already provided by the ping.application fields:
    # "appName": ["build", "applicationName"],
    # "appBuildID": ["build", "buildId"],
    # "appVersion": ["build", "version"],
    # "arch": ["build", "architecture"],
    # environment.system section
    "cpucount": ["system", "cpu", "count"],
    "memsize": ["system", "memoryMB"],
    # environment.system.device section
    "device": ["system", "device", "model"],
    "manufacturer": ["system", "device", "manufacturer"],
    "hardware": ["system", "device", "hardware"],
    "tablet": ["system", "device", "isTablet"],
    # environment.settings section
    "locale": ["settings", "locale"],
    # environment.system.os section
    "OS": ["system", "os", "name"],
    "version": ["system", "os", "version"],
    "kernel_version": ["system", "os", "kernelVersion"],
    # environment.system.gfx section
    "D2DEnabled": ["system", "gfx", "D2DEnabled"],
    "DWriteEnabled": ["system", "gfx", "DWriteEnabled"],
    "DWriteVersion": ["system", "gfx", "DWriteVersion"],
    # environment.system.hdd section
    "profileHDDModel": ["system", "hdd", "profile", "model"],
    "profileHDDRevision": ["system", "hdd", "profile", "revision"],
    "binHDDModel": ["system", "hdd", "binary", "model"],
    "binHDDRevision": ["system", "hdd", "binary", "revision"],
    "winHDDModel": ["system", "hdd", "system", "model"],
    "winHDDRevision": ["system", "hdd", "system", "revision"],
    # environment.addons section
    "activeExperiment": ["addons", "activeExperiment", "id"],
    "activeExperimentBranch": ["addons", "activeExperiment", "branch"],
    "persona": ["addons", "theme", "id"],
}

# Maps unified-ping.environment.gfx.adapters[].* => classic-ping.info.adapter*
adapterFieldMap = {
    "adapterDescription": ["description"],
    "adapterVendorID": ["vendorID"],
    "adapterDeviceID": ["deviceID"],
    "adapterSubsysID": ["subsysID"],
    "adapterRAM": ["RAM"],
    "adapterDriver": ["driver"],
    "adapterDriverVersion": ["driverVersion"],
    "adapterDriverDate": ["driverDate"],
}
 
dimensionMap = {
    "reason": ""
}

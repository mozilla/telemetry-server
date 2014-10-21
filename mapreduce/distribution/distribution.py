from __future__ import division
import simplejson as json

def map(k, d, v, cx):
    parsed = json.loads(v)
    reason, appName, appUpdateChannel, appVersion, appBuildID, submission_date = d
    info = parsed['info']
    simple = parsed['simpleMeasurements']

    os = info['OS']
    arch = info['arch']
    version = info['version']
    cpucount = str(info['cpucount'])
    memsize = str(int(round(info['memsize']/1000.)))
    disk = info.get('binHDDModel', "")
    gpu_vendor = info.get('adapterVendorID', "NA").replace(" ", "-").replace(",", "-").replace("--", "-")

    if os == "WINNT":
        os = "Windows"

    if arch == "x86":
        arch = "32-bit"
    elif arch == "x86-64":
        arch = "64-bit"

    if gpu_vendor == "0x10de":
        gpu_vendor = "Nvidia-GPU"
    elif gpu_vendor == "0x1002":
        gpu_vendor = "AMD-GPU"
    elif gpu_vendor == "0x8086":
        gpu_vendor = "Intel-GPU"
    elif gpu_vendor.startswith("0x"):
        gpu_vendor = "Other"

    if len(disk) > 0:
        disk = "SSD" if "ssd" in disk.lower() else "HDD"
    else:
        disk = "NA"

    cx.write(("Firefox", appUpdateChannel, appVersion, os, version, arch, \
              memsize + "-GB", cpucount + "-cores", disk, gpu_vendor), 1)

def setup_reduce(cx):
    cx.field_separator = ","

def reduce(k, v, cx):
    map = __builtins__['map']
    cx.write(" ".join(map(str, k)), sum(v))

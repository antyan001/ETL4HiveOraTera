!#/usr/bin/python

import xml.etree.ElementTree as ET
import subprocess as SP
import shlex
import subprocess

cmd = "grep -A 1 dfs.ha.namenodes /etc/hadoop/conf/hdfs-site.xml" 
#cmd = "hdfs getconf -confKey dfs.nameservices"
args = shlex.split(cmp)
process = subprocess.Popen(args, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
stdout, stderr = procees.communicate()


if __name__ == "__main__":
    hdfsSiteConfigFile = "/etc/hadoop/conf/hdfs-site.xml"

    tree = ET.parse(hdfsSiteConfigFile)
    root = tree.getroot()
    hasHadoopHAElement = False
    activeNameNode = None
    for property in root:
        if "dfs.ha.namenodes" in property.find("name").text:
            hasHadoopHAElement = True
            nameserviceId = property.find("name").text[len("dfs.ha.namenodes")+1:]
            nameNodes = property.find("value").text.split(",")
            print("#"*50)
            print("namenode alias --> " + nameserviceId)
            print("namenodes list --> " + ", ".join(nameNodes))
            print("HOSTS...")
            for node in nameNodes:
                #get the namenode machine address then check if it is active node
                for n in root:
                    prefix = "dfs.namenode.rpc-address." + nameserviceId + "."
                    elementText = n.find("name").text             
                    if prefix in elementText:
                        nodeAddress = n.find("value").text.split(":")[0]  
                        print("\t" + nodeAddress)
#                         args = ["hdfs haadmin -ns " + nameserviceId + " -getServiceState " + node]  
#                         p = SP.Popen(args, shell=True, stdout=SP.PIPE, stderr=SP.PIPE)

#                         for line in p.stdout.readlines():
#                             if "active" in line.lower():
#                                 print("Active NameNode: " + node)
#                                 break;
#                         for err in p.stderr.readlines():
#                             print("Error executing Hadoop HA command: ",err)
                break            
    if not hasHadoopHAElement:
        print("Hadoop High-Availability configuration not found!")
#/usr/bin/env sh
LD3PASS=$(cat /home/$(whoami)/pass/userpswrd | sed 's/\r//g')
pass_hdfs=hdfs:///user/$(whoami)/kerberos_pswrd

if [ -d "${pass_hdfs}" ]; then
    printf "pass directory exist"
else
    hdfs dfs -mkdir -p ${pass_hdfs}
fi

if [ -s "${pass_hdfs}/krb.pswrd" ]; then
    printf "${LD3PASS}\n${LD3PASS}" | ipa-getkeytab -P -p $(whoami)@DF.SBRF.RU -k ~/keytab/krb.pswrd
    hdfs dfs -rm -f --skipTrash "${pass_hdfs}/krb.pswrd"
    hdfs dfs -put ~/pass/userpswrd "${pass_hdfs}/krb.pswrd" && \
    hdfs dfs -chmod 744 "${pass_hdfs}/krb.pswrd"
else
    printf "${LD3PASS}\n${LD3PASS}" | ipa-getkeytab -P -p $(whoami)@DF.SBRF.RU -k ~/keytab/krb.pswrd
    hdfs dfs -put -f ~/keytab/krb.pswrd "${pass_hdfs}/krb.pswrd" && \
    hdfs dfs -chmod 744 "${pass_hdfs}/krb.pswrd"
fi

kinit -kt ${pass_hdfs}/krb.pswrd  $(whoami)@DF.SBRF.RU

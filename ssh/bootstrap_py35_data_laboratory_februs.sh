#!/bin/bash

cd /home/$USER/ && mkdir bin

# python 27 install
# ln -s /data/machine_learning/anaconda2/bin/jupyter-notebook /home/$USER/bin/jupyter-notebook
# ln -s /data/machine_learning/anaconda2/bin/python /home/$USER//bin/python
# ln -s /data/machine_learning/anaconda2/bin/ipython /home/$USER//bin/ipython
# ln -s /data/machine_learning/anaconda2/bin/python /home/$USER/bin/python27

# python 35 install
ln -s /opt/cloudera/parcels/PYENV.ZNO0059623792-3.5.pyenv.p0.2/bin/python /home/$USER//bin/python35
ln -s /opt/cloudera/parcels/PYENV.ZNO0059623792-3.5.pyenv.p0.2/bin/ipython /home/$USER//bin/ipython35
ln -s /opt/cloudera/parcels/PYENV.ZNO0059623792-3.5.pyenv.p0.2/bin/pip /home/$USER/bin/pip35

#echo "export PATH=$PATH:$HOME/bin" >> /home/$USER/.bash_profile
sleep 1
#source /home/$USER/.bash_profile



#echo "export PIP_CONFIG_FILE=/home/$USER/pip/pip.conf" >> /home/$USER/.bash_profile
#source /home/$USER/.bash_profile

# jupyter notebook install
# jupyter-notebook --generate-config
# sleep 1

# sed -i -- "s/# c.NotebookApp.port = 8888/c.NotebookApp.port = 19919/g" /home/$USER/.jupyter/jupyter_notebook_config.py
# sed -i -- "s/# c.NotebookApp.ip = 'localhost'/c.NotebookApp.ip = '$HOSTNAME'/g" /home/$USER/.jupyter/jupyter_notebook_config.py
# sed -i -- "s/# c.NotebookApp.open_browser = True/c.NotebookApp.open_browser = False/g" /home/$USER/.jupyter/jupyter_notebook_config.py

#mkdir ~/logs

# bash promt
#echo "PS1='\[\e[1;33m\]\u@\h \w ->\n\[\e[1;36m\] \@ \d\$\[\e[m\] '" >> /home/$USER/.bash_profile
#source /home/$USER/.bash_profile

# python35 libs
#mkdir -p /home/$USER/python35-libs/lib/python3.5/site-packages
#echo "export PYTHONPATH=$PYTHONPATH:$HOME/python35-libs/lib/python3.5/site-packages" >> /home/$USER/.bash_profile
source /home/$USER/.bash_profile


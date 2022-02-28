#!/bin/bash
PASS=$(cat /home/$(whoami)/pass/userpswrd | sed 's/\r//g'); echo $PASS | kinit
cd /opt/workspace/$USER/notebooks/SOURCES_UPDATE/sourses/
echo "!!!RUN STEPBYSTEP SCRIPTS FOR EACH SCENARIO/-->"
echo "working with script aist_interaction_hist.py ..."
./aist_interaction_hist.py
echo "working with script ma_cmdm_ma_agreement.py ..."
./ma_cmdm_ma_agreement.py
echo "working with script ma_cmdm_ma_deal_new.py ..."
./ma_cmdm_ma_deal_new.py
echo "working with script ma_cmdm_ma_mmb_insight.py ..."
./ma_cmdm_ma_mmb_insight.py
echo "working with script ma_cmdm_ma_product_offer.py ..."
./ma_cmdm_ma_product_offer.py
echo "working with script ma_cmdm_ma_task.py ..."
./ma_cmdm_ma_task.py
echo "working with script sme_cdm_offer_nontop.py ..."
./sme_cdm_offer_nontop.py
echo "working with script sme_cdm_offer_priority.py ..."
./sme_cdm_offer_priority.py
echo "working with script sme_cdm_offer_proc_coe.py ..."
./sme_cdm_offer_proc_coe.py
echo "working with script sme_cdm_offer_to_insight.py ..."
./sme_cdm_offer_to_insight.py
echo "working with script sme_cdm_offer_toxic.py ..."
./sme_cdm_offer_toxic.py
echo "working with script sbx_batch_rewrite.py ..."
./sbx_batch_rewrite.py
#echo "working with script call_task_to_sendsay_update.py ..."
#./call_task_to_sendsay_update.py

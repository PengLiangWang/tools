/**
 * @file    mod_mbpay_register_pay_jnls.c
 * @brief   null
 * @author  luyu
 * @version 1.0
 * @date    2011-09-25
 */


/** @{ */

#include "enruntime.h"
#include "appfielddef.h"
#include "jfpaypub.h"
#include "EDB_SysCardBinNo.h"

/*add by jdw@20160504 for ɾ���ַ����еĿո� begin*/
static void delspace(char *str)
{
    if (str == NULL)
        return;

    char    *tmp = str;

    while (*str != '\0')
    {
        if (*str != ' ')
            *tmp++ = *str;
        str++;
    }
    *tmp = '\0';
}
/*add by jdw@20160504 for ɾ���ַ����еĿո� end*/

int GetMerchantIdByLocal(TServiceSession *session, PayJnls *jnls, char *branchId, char *merType, int *serNo)
{
	int			res;
	int			dataLen, serialNo, flag = 0, found = 0;
	char		province[20] = {0};
	char		city[20] = {0};
	long		amount = 0;
	Select_Info		selInfo;
	PayMerchant		payMerch;
	TB_Pay_Province_Amount_Ctrl		provinceCtrl;
	dataLen = 20;
	DataPoolGetString2(session->dataPool, TX_PROVINCE_NAME, province, &dataLen);
	dataLen = 20;
	DataPoolGetString2(session->dataPool, TX_CITY_NAME, city, &dataLen);

	DataPoolGetLong(session->dataPool, TX_ORDER_AMOUNT, &amount);
	/*add by Bob @20160630 for local_amount_control begin*/	
	memset(&provinceCtrl, 0, sizeof(TB_Pay_Province_Amount_Ctrl));
	res = DB_TB_Pay_Province_Amount_Ctrl_read_by_flagId_and_Province_and_status("111111", province, "0", &provinceCtrl);
	if( res && res != SQLNOTFOUND )
	{
		ELOG(ERROR, "[%s_%s]Open_select���ݿ�[TB_Pay_Province_Amount_Ctrl][flagId:%s,Province:%s,status:%s]ʧ��, ERR:%d", 
			session->servCode, session->sessionId, "111111", province, "0", res);
		SET_EXCEPTION(session, LOCAL, SYSTEM, RUN_TO_D, NULL, NULL);
		return -1;
	}
	else if( res == TTS_SUCCESS ) 
	{
		//ELOG(INFO,"dayAmount:%lld, dayUpLimit:%lld", provinceCtrl.dayAmount, provinceCtrl.dayUpLimit);
		if( provinceCtrl.dayAmount + amount > provinceCtrl.dayUpLimit && provinceCtrl.dayUpLimit > 0 )
		{
			memset(&provinceCtrl, 0, sizeof(TB_Pay_Province_Amount_Ctrl));
			res = DB_TB_Pay_Province_Amount_Ctrl_read_by_flagId_and_status("000000", "0", &provinceCtrl);
			if( res && res != SQLNOTFOUND )
			{
				ELOG(ERROR, "[%s_%s]Open_select���ݿ�[TB_Pay_Province_Amount_Ctrl][flagId:%s,status:%s]ʧ��, ERR:%d", 
						session->servCode, session->sessionId, "000000", "0", res);
				SET_EXCEPTION(session, LOCAL, SYSTEM, RUN_TO_D, NULL, NULL);
				return -1;
			}
			else if( res == TTS_SUCCESS )
			{
				strcpy(province, provinceCtrl.Province);
				ELOG(INFO,"Ĭ����ѭ����province:%s",province);
			}
			else
			{
				ELOG(ERROR,"δ����Ĭ����ѭ����[flagId:000000][status:0]");
				return -1;
			}
		}
		else
		{
			;//��ԭ������ѭ
		}
	}
	else
	{
		;//��ԭ������ѭ
	}
	//DataPoolPutString(session->dataPool, TX_PROVINCE_NAME2, province);
	
	/*add by Bob @20160630 for local_amount_control end*/	


	serialNo = *serNo;
	ELOG(INFO, "�������̻���ѭ[%s, %s]", province, city);
	city[0] = '\0';
	strcpy(city, "NULL");
	if(strlen(province) == 0)
		return 1;	

PAYMERCHANT_CYCLE:
	memset(&selInfo, 0x00, sizeof(Select_Info));
	/*	
	res = DB_PayMerchant_open_select_by_branchId_and_merType_and_province_and_city_and_merState_and_serNo_GE_order_by_serNo( \
		branchId, merType, province,city, "0", serialNo, &selInfo);
	*/
		
	res = DB_PayMerchant_open_select_by_branchId_and_merType_and_province_and_merState_and_serNo_GE_order_by_serNo(branchId, merType, province, "0", serialNo, &selInfo);
	if(res)
	{
		ELOG(ERROR, "[%s_%s]Open_select���ݿ�[PayMerchant][branchId:%s,MerType:%s,Province:%s,City:%s]ʧ��, ERR:%d", 
			session->servCode, session->sessionId, branchId, merType, province, city, res);
		SET_EXCEPTION(session, LOCAL, SYSTEM, RUN_TO_D, NULL, NULL);
		return -1;
	}
	while(1)
	{
		memset(&payMerch, 0x00, sizeof(PayMerchant));
		res = DB_PayMerchant_fetch_select(&selInfo, &payMerch);
		if(res != TTS_SUCCESS && res != SQLNOTFOUND)
		{
			DB_PayMerchant_close_select(&selInfo);
			ELOG(ERROR, "[%s_%s]Fetch Select���ݿ�[PayMerchant][branchId:%s,MerType:%s,Province:%s,City:%s]ʧ��, ERR:%d", 
				session->servCode, session->sessionId, branchId, merType, province, city, res);
			SET_EXCEPTION(session, LOCAL, SYSTEM, RUN_TO_D, NULL, NULL);
			return -1;
		}
		else if(res == SQLNOTFOUND)
		{
			DB_PayMerchant_close_select(&selInfo);
			if(flag == 0)
			{
				serialNo = 1;
				flag = 1;
				//ELOG(INFO, "Cycle Again");
				goto PAYMERCHANT_CYCLE;
			}
			else
			{
				ELOG(INFO, "û���ҵ��������̻���[branchId:%s,Province:%s,City:%s]", branchId, province, city);
				return 1;
			}
		}
		else
		{
			//ELOG(INFO, "Merchant: %s - %s - %s - %s - %d - %d", payMerch.branchId, payMerch.merType, payMerch.merchantId, payMerch.termId, payMerch.upLimit, payMerch.DayUpLimit);
			if(payMerch.upLimit == 0)
			{
				if(payMerch.DayUpLimit == 0)
				{
					found = 1;
					break;
				}
				else if(jnls->amount + payMerch.DayAmount <= payMerch.DayUpLimit)
				{
					found = 1;
					break;
				}
			}
			else
			{
				if(payMerch.DayUpLimit == 0)
				{
					if(jnls->amount <= payMerch.upLimit)
					{
						found = 1;
						break;
					}
				}
				else
				{
					if(jnls->amount <= payMerch.upLimit && jnls->amount + payMerch.DayAmount <= payMerch.DayUpLimit)
					{
						found = 1;
						break;
					}
				}
			}
		}
	}
	if(found == 1)
	{
		ELOG(INFO, "�������̻���[%s]", payMerch.merchantId);
		strcpy(jnls->hostMerchId, payMerch.merchantId);
		strcpy(jnls->hostTermId, payMerch.termId);
		*serNo = payMerch.serNo;
	}
	DataPoolPutString(session->dataPool, TX_PROVINCE_NAME2, payMerch.province);
	DB_PayMerchant_close_select(&selInfo);
	return 0;
}


/*�ض��̻���ѵ*/
static int GetSpecializeMerchantId(TServiceSession *session, PayJnls *jnls, char *merType, char *merStat, int cardTag, int *serNo)
{
	int				res;
	int				serialno;
	int				found = 0, flag = 0, dataLen;
	char			branchId[11] = {0};
	PayMerchant		payMerch;
	Select_Info		selInfo;
	
	dataLen = 10;
	DataPoolGetString2(session->dataPool, TX_PRE_BRANCH_ID, branchId, &dataLen);

	serialno = *serNo;
	memset(&selInfo, 0x00, sizeof(Select_Info));

_SPECIAL_CYCLE_:
	res = DB_PayMerchant_open_select_by_branchId_and_merType_and_mlevel_and_merState_LE_and_serNo_GE_order_by_serNo(branchId, merType, cardTag, merStat, serialno, &selInfo);
	if(res)
	{
		ELOG(ERROR, "[%s_%s]Open_Select���ݿ�[PayMerchant][branchId:%s,merType:%s,level:%d]ʧ��, ERR:%d", session->servCode,
			session->sessionId, branchId, merType, cardTag, res);
		SET_EXCEPTION(session, LOCAL, SYSTEM, RUN_TO_D, NULL, NULL);
		return -1;
	}
	while(1)
	{
		memset(&payMerch, 0x00, sizeof(PayMerchant));
		res = DB_PayMerchant_fetch_select(&selInfo, &payMerch);
		if(res != TTS_SUCCESS && res != SQLNOTFOUND)
		{
			DB_PayMerchant_close_select(&selInfo);
			ELOG(ERROR, "[%s_%s]Fetch_Select���ݿ�[PayMerchant][branchId:%s, merType:%s, level:%d]ʧ��, ERR:%d",session->servCode,
				session->sessionId, branchId, merType, cardTag, res);
			SET_EXCEPTION(session, LOCAL, SYSTEM, RUN_TO_D, NULL, NULL);
			return -1;
		}
		else if(res == SQLNOTFOUND)
		{
			DB_PayMerchant_close_select(&selInfo);
			if(flag == 0)
			{
				serialno = 1;
				flag = 1;
				goto _SPECIAL_CYCLE_;
			}
			else
			{
				ELOG(WARN, "û���ҵ����ƻ��̻���");
				return 1;
			}
		}
		else
		{
		 	if(payMerch.upLimit == 0)
			{
				if(payMerch.DayUpLimit == 0)
				{
					found = 1;
					break;
				}
				else if(jnls->amount + payMerch.DayAmount <= payMerch.DayUpLimit)
				{
					found = 1;
					break;
				}
			}
			else
			{
				if(payMerch.DayUpLimit == 0)
				{
					if(jnls->amount <= payMerch.upLimit)
					{
						found = 1;
						break;
					}
				}
				else
				{
					if(jnls->amount <= payMerch.upLimit && jnls->amount + payMerch.DayAmount <= payMerch.DayUpLimit)
					{
						found = 1;
						break;
					}
				}
			}	
		}
	}

	if(found == 1)
	{
		ELOG(INFO, "���ƻ��̻���: [%s]", payMerch.merchantId);
		strcpy(jnls->hostMerchId, payMerch.merchantId);
		strcpy(jnls->hostTermId, payMerch.termId);
		*serNo = payMerch.serNo;
	}	
	DB_PayMerchant_close_select(&selInfo);
	
	return 0;
}

#if     1       //add by jdw@20160504 for ����customerIdѡ���̻��� begin
/* ��ϵͳ���󷵻� TTS_SUCCESS,ϵͳ���󷵻� SERVICE_EXCEPTION(SYSTEM) */
static int GetMerchantIdByCustomer(TServiceSession *session, PayJnls *jnls, char *merType, char *branchId)
{
	int		            res, dataLen;
    PayTimeCtrl         payTimeCtrl;
    TB_Pay_User_Ctrl    payUserCtrl;
	PayMerchant			payMerchant;
    time_t              times;
    char                merchName[100] = {0};

    dataLen = 99;
    DataPoolGetString2(session->dataPool, TX_MERCHANT_NAME, merchName, &dataLen);

    memset(&payMerchant, 0x00, sizeof(PayMerchant));

    memset(&payTimeCtrl, 0x00, sizeof(PayTimeCtrl));
    res = DB_PayTimeCtrl_read_by_servCode_and_branchId(session->servCode, jnls->branchId, &payTimeCtrl);
    if( res != TTS_SUCCESS && res != SQLNOTFOUND )
    {
        ELOG(ERROR, "[����:%s_%s]Read���ݿ�[PayTimeCtrl][servCode: %s][branchid:%s]ʧ��,ERR:%d", \
                session->servCode, session->sessionId, session->servCode, jnls->branchId, res);
        return SERVICE_EXCEPTION(SYSTEM);
    }
    else if( res == SQLNOTFOUND )
    {
        ELOG(INFO, "����(%s)δ���ý���(%s)����", jnls->branchId, session->servCode);
        res = DB_PayTimeCtrl_read_by_servCode_and_branchId(session->servCode, "00000000", &payTimeCtrl);
        if( res != TTS_SUCCESS && res != SQLNOTFOUND )
        {
            ELOG(INFO, "[����:%s_%s]Read���ݿ�[PayTimeCtrl][servCode: %s][branchid:00000000]ʧ��,ERR:%d", 
                    session->servCode, session->sessionId, session->servCode, res);
            return SERVICE_EXCEPTION(SYSTEM);
        }
        else if( res == SQLNOTFOUND )
        {
            ELOG(INFO, "[����:%s_%s]Read���ݿ�[PayTimeCtrl][servCode: %s][branchid:00000000]δ�ҵ�,ERR:%d", 
                    session->servCode, session->sessionId, session->servCode, res);
            return TTS_SUCCESS; 
        }
    }

    if(strcmp(merType,"0000") != 0 )
    {
        time(&times);

        memset(&payUserCtrl, 0x00, sizeof(TB_Pay_User_Ctrl));
        res = DB_TB_Pay_User_Ctrl_read_by_customerId_and_payType(jnls->customerId, merType, &payUserCtrl);
        if( res != TTS_SUCCESS && res != SQLNOTFOUND )
        {
            ELOG(ERROR, "[����:%s_%s]Read���ݿ�[TB_Pay_User_Ctrl][customerId:%s][payType:%s]ʧ��,ERR:%d", 
                    session->servCode, session->sessionId, jnls->customerId, merType, res);
            return SERVICE_EXCEPTION(SYSTEM);
        }
        else if( res == TTS_SUCCESS )
        {
            ELOG(INFO, "ʱ���:%d, ����ʱ��:%d", times - payUserCtrl.timeStamp, payTimeCtrl.tmUserCtrl);
            if( times - payUserCtrl.timeStamp <= payTimeCtrl.tmUserCtrl )
            {
                strncpy( jnls->hostMerchId, payUserCtrl.merchantId, 15);
                strncpy( jnls->hostTermId, payUserCtrl.termId, 8);
                DataPoolPutString(session->dataPool, TX_MERCHANT_NAME, payUserCtrl.merchName);

				res = DB_PayMerchant_read_by_branchId_and_merchantId_and_termId(branchId, payUserCtrl.merchantId, payUserCtrl.termId, &payMerchant);
				if(res)
				{
					ELOG(ERROR, "[����:%s_%s]Read���ݿ�[PayMerchanta][branchId:%s][termId:%s][merType:%s][merchantId:%s]ʧ��,ERR:%d", 
							session->servCode, session->sessionId, branchId, payUserCtrl.termId, merType, payUserCtrl.merchantId, res);
					return SERVICE_EXCEPTION(SYSTEM);
				}
				DataPoolPutString(session->dataPool, TX_PROVINCE_NAME2, payMerchant.province);

                ELOG(INFO, "�����[TB_Pay_User_Ctrl]�������̻���: [%s][merType:%s][province:%s]", payUserCtrl.merchantId, payUserCtrl.payType, payMerchant.province);
                return TTS_SUCCESS;
            }
            else
            {
                memcpy(payUserCtrl.merchantId, jnls->hostMerchId, 15);
                memcpy(payUserCtrl.termId, jnls->hostTermId, 8);
                memcpy(payUserCtrl.merchName, merchName, 99);
                memcpy(payUserCtrl.txnTime,jnls->localDate, 8);
                memcpy(payUserCtrl.txnTime + 8,jnls->localTime, 6);
                payUserCtrl.timeStamp = times;

                res = DB_TB_Pay_User_Ctrl_update_by_customerId_and_payType(jnls->customerId, merType, &payUserCtrl);
                if (res != TTS_SUCCESS)
                {
                    dbRollback();
                    ELOG(ERROR, "[����:%s_%s]Update���ݿ�[TB_Pay_User_Ctrl][customerId:%s][payType:%s]ʧ��,ERR:%d", 
                            session->servCode, session->sessionId, jnls->customerId, merType, res);
                    return SERVICE_EXCEPTION(SYSTEM);
                }
                dbCommit();
            }
        }
        else
        {
            memcpy(payUserCtrl.customerId, jnls->customerId, 10);
            memcpy(payUserCtrl.payType, merType, 4);
            memcpy(payUserCtrl.merchantId, jnls->hostMerchId, 15);
            memcpy(payUserCtrl.termId, jnls->hostTermId, 8);
            memcpy(payUserCtrl.merchName, merchName, 99);
            memcpy(payUserCtrl.txnTime,jnls->localDate, 8);
            strncat(payUserCtrl.txnTime,jnls->localTime, 6);
            payUserCtrl.timeStamp = times;

            res = DB_TB_Pay_User_Ctrl_add(&payUserCtrl);
            if (res != TTS_SUCCESS)
            {
                dbRollback();
                ELOG(ERROR, "[����:%s_%s]add���ݿ�[TB_Pay_User_Ctrl][customerId:%s][payType:%s]ʧ��,ERR:%d",
                        session->servCode, session->sessionId, jnls->customerId, merType, res);
                return SERVICE_EXCEPTION(SYSTEM);
            }
            dbCommit();
        }
    }

    return TTS_SUCCESS;
}
#endif           //add by jdw@20160504 for ����customerIdѡ���̻��� end

/*���ֽ��׹̶��̻��� add By ZHuweijin@2015-12-11*/
static int GetFixMerchantByServCode(TServiceSession *session)
{
	int				res;
	PayMerchant		payMerch;
	char			branchId[11] = {'\0'};
	
	sprintf(branchId, "GD%s", session->servCode);
	
	memset(&payMerch, 0x00, sizeof(PayMerchant));
	res = DB_PayMerchant_read_by_branchId_and_merType_and_merState(branchId, "0000", "0", &payMerch);
	if(res != TTS_SUCCESS && res != SQLNOTFOUND)
	{
		ELOG(ERROR, "[%s_%s]Open_Select���ݿ�[PayMerchant][branch:%s]ʧ��, ERR:%d", session->servCode, 
			session->sessionId, branchId, res);
		SET_EXCEPTION(session, LOCAL, SYSTEM, RUN_TO_D, NULL, NULL);
		return -1;
	} 
	else if(res == SQLNOTFOUND)
	{
		ELOG(INFO, "����[%s]δ���ù̶��̻���", session->servCode);
		return 1;
	}

	DataPoolPutString(session->dataPool, TX_HOST_MERCHANT_ID, payMerch.merchantId);
    DataPoolPutString(session->dataPool, TX_HOST_TERMINAL, payMerch.termId);
	
	return 0;
}


static int translateCondition(TServiceSession *session, char *condition, char *conditionForReal)
{
   char           *plusEnd;
   char           *plusBegin;
   char            dataDictId[4] = {0};
   char           *tempVal;
   unsigned int    conditionLen = 0;
   int             res;


   plusBegin = condition;
   conditionLen = strlen(condition);
   while(plusBegin < condition + conditionLen)
   {
      memset(dataDictId, 0, sizeof(dataDictId));
      if((plusEnd = strchr(plusBegin, '+')) != NULL)
      {
         strncpy(dataDictId, plusBegin, plusEnd - plusBegin);
         plusBegin = plusEnd + 1;
      }
      else
      {
         strncpy(dataDictId, plusBegin, &(condition)[conditionLen] - plusBegin );
         plusBegin = &(condition)[conditionLen];
      }

      res = DataPoolGetString(session->dataPool, atoi(dataDictId), &tempVal, NULL);
      if(res != TTS_SUCCESS)
      {
         ELOG(ERROR, "[����:%s]��ȡ������[dataDictId]ʧ��,ERR:%d", session->servCode, res);
         return res;
      }

      strncat(conditionForReal, tempVal, strlen(tempVal));
      strncat(conditionForReal, "+", 1);
   }
   conditionForReal[strlen(conditionForReal) - 1] = '\0';

   return TTS_SUCCESS;
}

#if     1   /*add by jdw@20160401 for �������� begin*/
static int GetExc002Handle(TServiceSession *session, char *servCode, char *msg, char *ReturnMsg, char *ReturnInfo)
{
	PayResultHandle   payResultHandle;
	PayResultHistory  payResultHistory;
	char              conditionForReal[65] = {0};
	int res;

	memset(&payResultHandle, 0, sizeof(PayResultHandle));

	res = DB_PayResultHandle_read_by_servCode_and_msgCode(servCode, msg, &payResultHandle);
	if( res !=TTS_SUCCESS && res != SQLNOTFOUND )
	{
		ELOG(ERROR, "[����:%s_%s]Read���ݿ�[PayResultHandle][servCode:%s][msgCode:%s]ʧ��,ERR:%d",
				session->servCode, session->sessionId, servCode, msg, res);
		SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, NULL, NULL);
		return SERVICE_EXCEPTION(SYSTEM);
	}
	else if( res == TTS_SUCCESS )
	{
		memset(conditionForReal, 0x00, sizeof(conditionForReal));

		res = translateCondition(session, payResultHandle.limitCondition, conditionForReal);
		if(res != TTS_SUCCESS)
		{
			SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, NULL, NULL);
			return SERVICE_EXCEPTION(SYSTEM);
		}
		strcat(conditionForReal, "+D");
		ELOG(INFO, "conditionForReal: %s", conditionForReal);

		memset(&payResultHistory, 0, sizeof(PayResultHistory));
		res = DB_PayResultHistory_read_by_condition(conditionForReal, &payResultHistory);
		if( res != TTS_SUCCESS && res != SQLNOTFOUND )
		{
			ELOG(ERROR, "[����:%s_%s]Read���ݿ�[PayResultHistory][condition:%s]ʧ��,ERR:%d",
					session->servCode, session->sessionId, conditionForReal, res);
			SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, NULL, NULL);
			return SERVICE_EXCEPTION(SYSTEM);
		}
		else if( res == TTS_SUCCESS )
		{
			if( payResultHistory.countNum >= payResultHandle.cautionNum )
			{
				ELOG(ERROR, "�ý��׳������޴���");
				SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, ReturnMsg, ReturnInfo);
				return SERVICE_EXCEPTION(SYSTEM);
			}
		}
	}

	return TTS_SUCCESS;
}
#endif      /*add by jdw@20160401 for �������� end*/


static int invoke_mbpay_register_pay_jnls(TServiceSession *session)
{
   int                  res, iRet,dataLen;
   char					shopNo[23] = {0};
   char                 tradeTime[7] = {0};
   TradeInfo            sTradeInfo;
   TradeInfo            sUpBranchInfo;
   //long                 lcount = 0;
   PayJnls             *jnls ;
   TLockInfo            lockInfo;
   PayCustomer          payCustomer;
   PayUser              payUser;
   PayReject            payReject;
   char              conditionForReal[65] = {0};
   char              blackreason1[40]={0};
   char              blackreason2[40]={0};
   long               blackflag1 = 0;
   long               blackflag2 = 0;
   char              catag[2] = {0};
   PayResultHandle   payResultHandle;
   PayResultHistory  payResultHistory;
   PayBlackList      payBlackList;
   PayBlackJnls      payBlackJnls;
   PayBlackList      payBlackListAdd;
   TCardBin          cardBin;
   Select_Info       Sinfo;
   PayCardTradeCtrl  payCardTradeCtrl;

   ELOG(INFO, "����ģ�� mod_mbpay_register_pay_jnls ");

   strncpy((session->sessionId) + 8, tradeTime, 6);

   res = EngineServicePrivateDataMake(session, (char **)&jnls, sizeof(PayJnls));
   if ( res )
   {
      ELOG(ERROR, "[����:%s]��������˽��������ʧ��,ERR:%d", session->servCode, res);
      SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_A, NULL, NULL);
      return SERVICE_EXCEPTION(SYSTEM);
   }

   strncpy(jnls->servCode, session->servCode,  6);
   strncpy(jnls->tradeCode, session->current->headerIn->tradeCode, 6);
   strncpy(jnls->localDate, session->headerOut->trnTxDate, 8);
   strncpy(jnls->localTime, session->headerOut->trnTxTime, 6);
   strncpy(jnls->localLogNo, session->headerOut->trnTxLogNo, 6);
   strncpy(jnls->localSettlmtDate, session->headerOut->trnTxDate, 8);

   strncpy(jnls->mobileno, session->headerIn->terminalId, 11);

   strncpy(jnls->channelId, session->headerIn->channelId, 4);
   dataLen = 24;
   DataPoolGetString2(session->dataPool, TX_TERMINAL_ID, jnls->termId, &dataLen);

   dataLen = 16;
   DataPoolGetString2(session->dataPool, TX_PSAM_ID, jnls->psamId, &dataLen);

   dataLen = 32;
   DataPoolGetString2(session->dataPool, TX_ORDER_NO,jnls->orderId, &dataLen);

   dataLen = 2;
   DataPoolGetString2(session->dataPool, TX_PAY_TYPE, jnls->payType, &dataLen);

   dataLen = 12;
   DataPoolGetString2(session->dataPool, ACC_SUBJECTID, jnls->branchId, &dataLen);
   // strncpy(jnls->branchId, session->headerIn->branchId, 12);

   strncpy(jnls->orgTxLogNo, session->headerIn->orgTxLogNo, 6);
   strncpy(jnls->orgTxRefNo, session->headerIn->orgTxRefNo, 24);
   strncpy(jnls->orgTxDate, session->headerIn->orgTxDate, 8);
   strncpy(jnls->orgTxTime, session->headerIn->orgTxTime, 6);
   strncpy(jnls->orgSettlmtDate, session->headerIn->orgTxDate, 8);
   strncpy(jnls->sndTxLogNo, session->headerIn->sndTxLogNo, 6);
   strncpy(jnls->sndTxDate, session->headerIn->sndTxDate, 8);
   strncpy(jnls->sndTxTime, session->headerIn->sndTxTime, 6);

   //add by li.q@20151130 for �����ظ� end
   ELOG(INFO, "session->current->trade->store[%d]", session->current->trade->store);
   if(session->current->trade->store == 1)
   		DataPoolPutString(session->dataPool, TX_IF_PAYMENT, "Y");//�жϵ�ǰtradecode�Ƿ�Ϊ�ۿ���
   else
		DataPoolPutString(session->dataPool, TX_IF_PAYMENT, "N");
   //add by li.q@20151130 for �����ظ� end
   
   dataLen = 32;
   res = DataPoolGetString2(session->dataPool, TX_ACCOUNT, jnls->account, &dataLen);
   if(res != TTS_SUCCESS)
   {
      DataPoolGetString2(session->dataPool, TX_ACCT_TYPE, jnls->account, &dataLen);
   }

   dataLen = 48;

   DataPoolGetString2(session->dataPool, TX_ACCOUNT2, jnls->account2, &dataLen);


   DataPoolGetLong(session->dataPool, TX_ORDER_AMOUNT, (long *)&jnls->amount);

   res = DataPoolGetLong(session->dataPool, TX_AMOUNT, (long *)&jnls->fee);
   if(res == TTS_SUCCESS)
   {
      jnls->fee = jnls->fee - jnls->amount;
   }

   dataLen = 12;
   DataPoolGetString2(session->dataPool, TX_HOST_BRANCH_ID, jnls->hostBranchId, &dataLen);

   	strncpy(jnls->status, "1", 1);
   	dataLen = 6;
   	DataPoolGetString2(session->dataPool, TX_MP_MOBILE_MAC, jnls->checkCode, &dataLen);
   	ELOG(INFO, "SERVCODE[%s]TRADECODE[%s]", jnls->servCode, jnls->tradeCode);
   	//add by li.q@20150326 for ��ֵ���� paytag begin
   	if( (strcmp("100085", jnls->servCode) == 0 && strcmp("GYG002", jnls->tradeCode) == 0)  ||  
                   (strcmp("200030", jnls->servCode) == 0 && strcmp("GYG002", jnls->tradeCode) == 0) ||
                   (strcmp("200044", jnls->servCode) == 0 && strcmp("OFC001", jnls->tradeCode) == 0) ||
                   (strcmp("200045", jnls->servCode) == 0 && strcmp("OFC001", jnls->tradeCode) == 0) ||
                   (strcmp("100020", jnls->servCode) == 0 && strcmp("OFC001", jnls->tradeCode) == 0) ||
                   (strcmp("100022", jnls->servCode) == 0 && strcmp("OFC001", jnls->tradeCode) == 0) )//�ֻ���ֵ������ֵ����
   	{
   		strncpy(jnls->payTag , "A", 1);
   	}
   	else
   	{
        strncpy(jnls->payTag , "0", 1);
   	}
   	//add by li.q@20150326 for ��ֵ���� paytag end
   
   //add by wb_20150327 for ��ӿ��֣������� begin
   char cardType[3]={0}; //������
   char cardType2[3]={0}; //����
   dataLen = 2;
   res = DataPoolGetString2(session->dataPool, TX_CARD_TYPE2, cardType, &dataLen);
   if (res != TTS_SUCCESS)
   {
           ELOG(INFO, "[����:%s]��ȡ������[TX_CARD_TYPE2]ʧ��,ERR:%d", session->servCode, res);
   }

   memset(&cardBin, 0, sizeof(TCardBin));
   res = TrieExGet(session->resource->engine->cardBin, jnls->account, &cardBin, NULL);
   if (res != TTS_SUCCESS)
   {
           ELOG(INFO, "[����:%s]��ȡ�����ʺ�[account:%s]��cardBin��ʧ��,ERR:%d", session->servCode, jnls->account, res); 
   }
   strncpy(cardType2, cardBin.cardType, 2);
   if(strncmp(cardType, "10", 2) == 0 )   //��������
   {
           if(strncmp(cardType2, "03", 2) == 0 )  //���ÿ�
           {
                   strncpy(jnls->issuer, "00",2);      //�������ÿ�
           }
           else if(strncmp(cardType2, "01", 2) == 0 ) //��ǿ�
           {
                   strncpy(jnls->issuer, "01", 2);     //���Ž�ǿ�
           }
           else
           {
                   strncpy(jnls->issuer, "05", 2);     //����δ֪��
           }
   }
   else if(strncmp(cardType, "11", 2) == 0 || strncmp(cardType, "12", 2) == 0)    //IC��
   {
           if(strncmp(cardType2, "03", 2) == 0 )  //���ÿ�
           {
                   strncpy(jnls->issuer, "10",2);      //IC���ÿ�
           }
           else if(strncmp(cardType2, "01", 2) == 0 ) //��ǿ�                                                         
           {                                                                                                           
                   strncpy(jnls->issuer, "11", 2);     //IC��ǿ�                                                      
           }                                                                                                           
           else
           {                                                                                                           
                   strncpy(jnls->issuer, "15", 2);     //ICδ֪��                                                      
           }                                                                                                           
   }                                                                                                                
   else if(strncmp(cardType, "13", 2) == 0 )    //add by jdw@20160612 for NFC   jnls->issuer[0] = '2'
   {
           if(strncmp(cardType2, "03", 2) == 0 )  //���ÿ�
           {
                   strncpy(jnls->issuer, "20",2);      //���ÿ�
           }
           else if(strncmp(cardType2, "01", 2) == 0 ) //��ǿ�                                                         
           {                                                                                                           
                   strncpy(jnls->issuer, "21", 2);     //��ǿ�                                                      
           }                                                                                                           
           else
           {                                                                                                           
                   strncpy(jnls->issuer, "25", 2);     //δ֪��                                                      
           }                                                                                                           
   }
   else                                                                                                             
   {                                                                                                                
           if(strncmp(cardType2, "03", 2) == 0 )  //���ÿ�                                                             
           {                                                                                                           
                   strncpy(jnls->issuer, "50",2);      //δ֪���ÿ�                                                    
           }                                                                                                           
           else if(strncmp(cardType2, "01", 2) == 0 ) //��ǿ�                                                         
           {                                                                                                           
                   strncpy(jnls->issuer, "51", 2);     //δ֪��ǿ�                                                    
           }                                                                                                           
           else                                                                                                        
           {                                                                                                           
                   strncpy(jnls->issuer, "55", 2);     //δ֪��                                                        
           }                                                                                                           
   }
	
   //add by wb_20150327 for ��ӿ��֣������� end
   DataPoolPutString(session->dataPool, TX_ISSUER_TYPE, jnls->issuer); // add by Zhuweijin@20151021

   dataLen = 128; 
   if(strncmp(session->servCode, "100058", 6) == 0 || strncmp(session->servCode, "100059", 6) == 0)
   {
        char t_remark[130] = {0}; 
        DataPoolGetString2(session->dataPool, TX_TRANS_DESC, t_remark, &dataLen);
		ELOG(INFO,"tx_trans_desc:%s",t_remark);
        char *t_remark_1 = strstr(t_remark, "|");
		if( !t_remark_1 )
		{
			SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, NULL, NULL);
			return SERVICE_EXCEPTION(SYSTEM);
		}
        char *t_remark_2 = strstr(t_remark_1+1, "|");
		if( !t_remark_2 )
		{
			SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, NULL, NULL);
			return SERVICE_EXCEPTION(SYSTEM);
		}
        char *t_remark_3 = strstr(t_remark_2+1, "|");
		if( !t_remark_3 )
		{
			SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, NULL, NULL);
			return SERVICE_EXCEPTION(SYSTEM);
		}
        memset(&jnls->account2, 0, sizeof(jnls->account2));
        strncpy(jnls->account2, t_remark_1+1, strlen(t_remark_1) - strlen(t_remark_2) -1); 
        strncpy(jnls->remark, t_remark_2+1, strlen(t_remark_2) - strlen(t_remark_3) -1); 
   }
   else 
   {
        DataPoolGetString2(session->dataPool, TX_TRANS_DESC, jnls->remark, &dataLen);
   } 

   dataLen = 10;
   DataPoolGetString2(session->dataPool, TX_MERCHANT_ID, jnls->merchantId, &dataLen);

   dataLen = 10;
   DataPoolGetString2(session->dataPool, TX_PRODUCT_ID, jnls->productId, &dataLen);

   memset(&sTradeInfo, 0, sizeof(sTradeInfo));
   memset(&sUpBranchInfo, 0, sizeof(sUpBranchInfo));

   dataLen = 32;
   if(strncmp(session->servCode, "100032", 6) == 0 )
   {
           DataPoolGetString2(session->dataPool, TX_ACCOUNT2, jnls->customerId, &dataLen);
   }
   else
   {
           DataPoolGetString2(session->dataPool, TX_CUSTOMER_ID, jnls->customerId, &dataLen);

   }

   memset(&payCustomer, 0, sizeof(PayCustomer));
   memset(&payUser, 0, sizeof(PayUser));
   memset(&payReject, 0, sizeof(PayReject));

   ELOG(INFO, "[mobileno:%s]", jnls->mobileno);

   res = DB_PayUser_read_by_userId_and_branchId(jnls->mobileno, jnls->branchId, &payUser);
   if(res != TTS_SUCCESS)
   {
           ELOG(ERROR, "[����:%s_%s]Read���ݿ��[PayUser][mobileno:%s][branchId:%s]ʧ��,ERR:%d", \
                session->servCode, session->sessionId, jnls->mobileno, jnls->branchId, res);
           SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, NULL, NULL);
           return SERVICE_EXCEPTION(SYSTEM);
   }

   res = DB_PayCustomer_read_by_customerId(payUser.customerId, &payCustomer);
   if(res != TTS_SUCCESS)
   {
           ELOG(ERROR, "[����:%s_%s]Read���ݿ��[PayCustomer][customerId:%s]ʧ��,ERR:%d", \
                session->servCode, session->sessionId, jnls->customerId, res);
           SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, NULL, NULL);
           return SERVICE_EXCEPTION(SYSTEM);
   }

   ELOG(INFO, "customerId[%s]",payUser.customerId);
   ELOG(INFO, "branchId[%s]",jnls->branchId);


   	res = DB_PayReject_read_by_branchId(jnls->branchId, &payReject);
   	if(res != TTS_SUCCESS && res != SQLNOTFOUND)
   	{
   		ELOG(ERROR, "[����:%s_%s]Read���ݿ��[PayReject][branchId:%s]ʧ��,ERR:%d", session->servCode, session->sessionId, jnls->branchId, res);
        SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, NULL, NULL);
       	return SERVICE_EXCEPTION(SYSTEM);
   	}
   	else if (res == TTS_SUCCESS)
   	{
        ELOG(INFO, "notAuthLimit[%d]  payCustomer.customerTag[0][%s]", payReject.notAuthLimit, payCustomer.customerTag); 
        if(payReject.notAuthLimit == 1 && session->service->store == 2 && payCustomer.customerTag[0] != '3')
        {
		   	ELOG(ERROR, "[����:%s]�û�[customerId:%s]���Ƴ���", session->servCode, payCustomer.customerId);  
		   	SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, "31", "δͨ��ʵ����֤�̻�����֧�ִ˽���");
		   	return SERVICE_EXCEPTION(SYSTEM);
        }
        else if (payReject.notAuthLimit == 2 && payCustomer.customerTag[0] != '3')
        {
		   	ELOG(ERROR, "[����:%s]�û�[customerId:%s]���ƽ���", session->servCode, payCustomer.customerId); 
		   	SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, "31", "δͨ��ʵ����֤�̻�����֧�ִ˽���");
		   	return SERVICE_EXCEPTION(SYSTEM);
        }
   	}
   	else
   	{
    	ELOG(INFO, "�����ƽ���");                                
   	}

   /*add by wpl@�����ֿغ�������� begin*/
   PayRiskControl2   riskcontrol;

    /*��bin���� add By Zhuweijin@20160616 begin*/
    memset(&Sinfo, 0, sizeof(Sinfo));
    riskcontrol.flag = 2; 
    res = DB_PayRiskControl2_open_select_by_flag(riskcontrol.flag, &Sinfo);
    if(res)
    {
        ELOG(INFO, "[����:%s_%s]Open_Select���ݿ�[PayRiskControl2][flag:%s]ʧ��, ERR:%d", session->servCode, 
			session->sessionId, riskcontrol.flag, res);
        SET_EXCEPTION(session, LOCAL, SYSTEM, RUN_TO_D, NULL, NULL);
        return SERVICE_EXCEPTION(SYSTEM);
    } 

    while(1)
    {
        memset(&riskcontrol, 0x00, sizeof(riskcontrol));
        res = DB_PayRiskControl2_fetch_select(&Sinfo, &riskcontrol);
        if(res == SQLNOTFOUND)
        {
            ELOG(INFO, "�ÿ�[%s]û�н�������", jnls->account);
            DB_PayRiskControl2_close_select(&Sinfo);
            break;
        }
        
        if(res)
        {
            ELOG(INFO, "[����:%s_%s]Fetch���ݿ�[PayRiskControl2]ʧ��, ERR:%d", session->servCode, session->sessionId, res);
            DB_PayRiskControl2_close_select(&Sinfo);
            break;
        }

	    if (strncmp(jnls->account, riskcontrol.MsgAcct, riskcontrol.AcctLen) == 0)
	    {
		    ELOG(ERROR, "�ÿ���[%s]�ܷ������", jnls->account);
            DB_PayRiskControl2_close_select(&Sinfo);
            SET_EXCEPTION(session, LOCAL, SYSTEM, RUN_TO_D, "D4", "�ݲ�֧�ָÿ�");
            return SERVICE_EXCEPTION(SYSTEM);
	    }
    }
    /*add By Zhuweijin@2016 end*/

   long              upblackflag1 = 0, upblackflag2 = 0;

   memset(&riskcontrol, 0, sizeof(riskcontrol));
   riskcontrol.flag = 1;     //���ݿ���ǰ��λ�ж��Ƿ�����������־λ

   memset(&Sinfo, 0, sizeof(Sinfo)); 
   res = DB_PayRiskControl2_open_select_by_flag(riskcontrol.flag, &Sinfo);
   if(res != TTS_SUCCESS)
   {
       ELOG(INFO, "[����:%s_%s]Open_Select���ݿ�[PayRiskControl2][flag:%s]ʧ��, ERR:%d", session->servCode, 
			session->sessionId, riskcontrol.flag, res);
       goto __LOOP__;
   } 

   while(1)
   {
       memset(&riskcontrol, 0, sizeof(riskcontrol));

       res = DB_PayRiskControl2_fetch_select(&Sinfo, &riskcontrol);
       if(res != TTS_SUCCESS && res != SQLNOTFOUND)
       {
           ELOG(INFO, "[����:%s_%s]Fetch���ݿ�[PayRiskControl2]ʧ��, ERR:%d", session->servCode, session->sessionId, res);
           DB_PayRiskControl2_close_select(&Sinfo);
           goto __LOOP__;
       }
       else if(res == SQLNOTFOUND)
       {
           ELOG(INFO, "[����:%s]û����Ӧ�ķ������", session->servCode);
           DB_PayRiskControl2_close_select(&Sinfo);
           goto __LOOP__;
       }
	   if ( strncmp(jnls->account, riskcontrol.MsgAcct, riskcontrol.AcctLen) != 0 )
	   {
		   continue;
	   }
	   DB_PayRiskControl2_close_select(&Sinfo);
   
        memset(&Sinfo, 0, sizeof(Sinfo)); 
        memset(&payBlackList, 0, sizeof(PayBlackList));
        res = DB_PayBlackList_read_by_blackKey_and_blackType(jnls->account, "03", &payBlackList);
        if(res == TTS_SUCCESS)
        {
            ELOG(INFO, "[����:%s]�ÿ����ں����������Ѵ���.", session->servCode);
            DB_PayRiskControl2_close_select(&Sinfo);
            goto __LOOP__;
        }
        if(res != TTS_SUCCESS && res != SQLNOTFOUND)
        {
            ELOG(ERROR, "[����:%s_%s] Read ���ݿ��[PayBlackList][blackKey:%s][blackType:03]ʧ��,ERR:%d", session->servCode, 
				session->sessionId, jnls->account, res);
            DB_PayRiskControl2_close_select(&Sinfo);
            SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, NULL, NULL);
            return SERVICE_EXCEPTION(SYSTEM);
        }

        res = DB_Sequence_read_by_name("SEQ_BLACKLIST_FLAG", (long *)&upblackflag1);
        if(res != TTS_SUCCESS)
        {
            DB_PayRiskControl2_close_select(&Sinfo);
            ELOG(ERROR,"[����:%s_%s]Read���ݿ�[Sequence][name:SEQ_BLACKLIST_FLAG]ʧ��, ERR:%d",
            session->servCode, session->sessionId, res);
            SET_EXCEPTION(session, LOCAL, SYSTEM, RUN_TO_D, NULL, NULL);
            return SERVICE_EXCEPTION(SYSTEM);
        }
        ELOG(INFO, "upblackflag1 = %d", upblackflag1);
        
        memset(&payBlackListAdd, 0, sizeof(payBlackListAdd));
        memset(&payBlackJnls, 0, sizeof(payBlackJnls));

        memcpy(payBlackListAdd.blackKey,jnls->account, strlen(jnls->account));
        memcpy(payBlackListAdd.blackType,"03", 2);
        payBlackListAdd.blackLevel = 1;
        memcpy(payBlackListAdd.blackTag,"0", 1);
        payBlackListAdd.flag = upblackflag1;

        //DB_PayBlackList_debug_print("ERROR", &payBlackListAdd, __FILE__, __LINE__);

        res = DB_PayBlackList_add(&payBlackListAdd);
        if( res )
        {
                dbRollback();
                DB_PayRiskControl2_close_select(&Sinfo);
                ELOG(ERROR, "[����:%s_%s]Add���ݿ��[PayBlackList][blackKey:%s]ʧ��,ERR:%d", session->servCode, 
					session->sessionId, jnls->account, res);
                SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, NULL, NULL);
                return SERVICE_EXCEPTION(SYSTEM);
        }
        dbCommit();

        memcpy(payBlackJnls.localDate, jnls->localDate, strlen(jnls->localDate));
        memcpy(payBlackJnls.localTime, jnls->localTime, strlen(jnls->localTime));
        memcpy(payBlackJnls.servCode, session->servCode, strlen(session->servCode));
        memcpy(payBlackJnls.customerId, jnls->customerId, strlen(jnls->customerId));
        memcpy(payBlackJnls.blackKey, jnls->account, strlen(jnls->account));
        memcpy(payBlackJnls.blackType, "03", 2);
        payBlackJnls.blackLevel = 1;
        memcpy(payBlackJnls.blackReason, "���п����쳣", 40);
        payBlackJnls.flag = upblackflag1;

        res = DB_PayBlackJnls_add(&payBlackJnls);
        if( res )
        {
        	dbRollback();
            DB_PayRiskControl2_close_select(&Sinfo);
            ELOG(ERROR, "[����:%s_%s]Add���ݿ��[payBlackJnls][blackKey:%s]ʧ��,ERR:%d", session->servCode, 
				session->sessionId, jnls->account, res);
            SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, NULL, NULL);
            return SERVICE_EXCEPTION(SYSTEM);
        }
        dbCommit();

     
        //��Ӧ�˺�Ҳ���������
        res = DB_Sequence_read_by_name("SEQ_BLACKLIST_FLAG", (long *)&upblackflag2);
        if(res != TTS_SUCCESS)
        {
            DB_PayRiskControl2_close_select(&Sinfo);
            ELOG(ERROR,"[����:%s_%s]Read���ݿ�[Sequence][name:SEQ_BLACKLIST_FLAG]ʧ��, ERR:%d",
            session->servCode, session->sessionId, res);
            SET_EXCEPTION(session, LOCAL, SYSTEM, RUN_TO_D, NULL, NULL);
            return SERVICE_EXCEPTION(SYSTEM);
        }

        memset(&payBlackList, 0, sizeof(PayBlackList));
        res = DB_PayBlackList_read_by_blackKey_and_blackType(jnls->mobileno, "01", &payBlackList);
        if(res == TTS_SUCCESS)
        {
            ELOG(INFO, "���˺��ں����������Ѵ���");
            DB_PayRiskControl2_close_select(&Sinfo);
            goto __LOOP__;
        }
        if(res != TTS_SUCCESS && res != SQLNOTFOUND)
        {
            DB_PayRiskControl2_close_select(&Sinfo);
            ELOG(ERROR, "[����:%s_%s] Read ���ݿ��[PayBlackList][blackKey:%s][blackType:01]ʧ��,ERR:%d", session->servCode, 
				session->sessionId, jnls->mobileno, res);
            SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, NULL, NULL);
            return SERVICE_EXCEPTION(SYSTEM);
        }
                
        memset(&payBlackListAdd, 0, sizeof(payBlackListAdd));
        memset(&payBlackJnls, 0, sizeof(payBlackJnls));

        memcpy(payBlackListAdd.blackKey,jnls->mobileno, strlen(jnls->mobileno) );
        memcpy(payBlackListAdd.blackType,"01", 2);
        payBlackListAdd.blackLevel = 1;
        memcpy(payBlackListAdd.blackTag,"0", 1);
        payBlackListAdd.flag = upblackflag2;
        payBlackListAdd.upperflag = upblackflag1;


        res = DB_PayBlackList_add(&payBlackListAdd);
        if( res )
        {
        	dbRollback();
            DB_PayRiskControl2_close_select(&Sinfo);
            ELOG(ERROR,"[����:%s_%s]Add���ݿ��[PayBlackList][blackKey:%s]ʧ��,ERR:%d",session->servCode, 
				session->sessionId, jnls->mobileno, res);
            SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, NULL, NULL);
            return SERVICE_EXCEPTION(SYSTEM);
        }
        dbCommit();

        strcpy(blackreason1, "���ڿ���: ");
        strncat(blackreason1, jnls->account, 20);

        memcpy(payBlackJnls.localDate, jnls->localDate, strlen(jnls->localDate));
        memcpy(payBlackJnls.localTime, jnls->localTime, strlen(jnls->localTime));
        memcpy(payBlackJnls.servCode, session->servCode, strlen(session->servCode));
        memcpy(payBlackJnls.customerId, jnls->customerId, strlen(jnls->customerId));
        memcpy(payBlackJnls.blackKey, jnls->mobileno, strlen(jnls->mobileno));
        memcpy(payBlackJnls.blackType, "01", 2);
        payBlackJnls.blackLevel = 1;
        memcpy(payBlackJnls.blackReason, blackreason1, 40);
        payBlackJnls.flag = upblackflag2;
        payBlackJnls.upperflag = upblackflag1;
        memcpy(payBlackJnls.upblackType, "03", 2);
        memcpy(payBlackJnls.upblackKey, jnls->account, strlen(jnls->account));

        res = DB_PayBlackJnls_add(&payBlackJnls);
        if( res )
        {
        	dbRollback();
            DB_PayRiskControl2_close_select(&Sinfo);
            ELOG(ERROR, "[����:%s_%s]Add���ݿ��[payBlackJnls][blackKey:%s]ʧ��,ERR:%d", session->servCode, 
				session->sessionId, jnls->mobileno, res);
            SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, NULL, NULL);
            return SERVICE_EXCEPTION(SYSTEM);
        }
        dbCommit();

        DB_PayRiskControl2_close_select(&Sinfo);
        ELOG(INFO, "[����:%s]�û�[mobileno:%s account:%s]�Ѽ��������", session->servCode, jnls->mobileno, jnls->account);
        SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, "31", "�������ޣ�����ϵ������");
        return SERVICE_EXCEPTION(SYSTEM);
   }
   /*add by wpl@�������ֿع��� end*/   


__LOOP__:
   memset(blackreason1, 0x00, sizeof(blackreason1));
   memset(&payBlackList, 0, sizeof(PayBlackList));

   res = DB_PayBlackList_read_by_blackKey_and_blackType(jnls->mobileno ,"01", &payBlackList);
   blackflag1 = payBlackList.flag;

   memset(&payBlackList, 0, sizeof(PayBlackList));

   iRet = DB_PayBlackList_read_by_blackKey_and_blackType(jnls->account ,"03", &payBlackList);
   blackflag2 = payBlackList.flag;

   int iLen = 0;
   iLen=strlen(jnls->account);
   if(iLen <= 8)
           iRet = 1;


   if(res == 0 || iRet == 0)       
   {
           if(res == SQLNOTFOUND)
           {
                   upblackflag1 = 0;
                   res = DB_Sequence_read_by_name("SEQ_BLACKLIST_FLAG", (long *)&upblackflag1);
                   if(res != TTS_SUCCESS)
                   {
                       ELOG(ERROR,"[����:%s_%s]Read���ݿ�[Sequence][name:SEQ_BLACKLIST_FLAG]ʧ��, ERR:%d",
                           session->servCode, session->sessionId, res);
                       SET_EXCEPTION(session, LOCAL, SYSTEM, RUN_TO_D, NULL, NULL);
                       return SERVICE_EXCEPTION(SYSTEM);
                   }

                   memset(&payBlackListAdd, 0, sizeof(payBlackListAdd));
                   memset(&payBlackJnls, 0, sizeof(payBlackJnls));

                   memcpy(payBlackListAdd.blackKey,jnls->mobileno, strlen(jnls->mobileno) );
                   memcpy(payBlackListAdd.blackType,"01", 2);
                   payBlackListAdd.blackLevel = 1;
                   memcpy(payBlackListAdd.blackTag,"0", 1);
                   payBlackListAdd.flag = upblackflag1;
                   payBlackListAdd.upperflag = blackflag2;

                   res = DB_PayBlackList_add(&payBlackListAdd);
                   if( res )
                   {
                           dbRollback();
                           ELOG(ERROR,"[����:%s_%s]Add���ݿ��[PayBlackList][blackKey:%s]ʧ��,ERR:%d",session->servCode, session->sessionId, jnls->mobileno, res);
                           SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, NULL, NULL);
                           return SERVICE_EXCEPTION(SYSTEM);
                   }

                   strcpy(blackreason1,"���ڿ��ţ�");
                   strncat(blackreason1,jnls->account,20);
                   memcpy(payBlackJnls.localDate, jnls->localDate, strlen(jnls->localDate));
                   memcpy(payBlackJnls.localTime, jnls->localTime, strlen(jnls->localTime));
                   memcpy(payBlackJnls.servCode, session->servCode, strlen(session->servCode));
                   memcpy(payBlackJnls.customerId, jnls->customerId, strlen(jnls->customerId));
                   memcpy(payBlackJnls.blackKey, jnls->mobileno, strlen(jnls->mobileno));
                   memcpy(payBlackJnls.blackType, "01", 2);
                   payBlackJnls.blackLevel = 1;
                   memcpy(payBlackJnls.blackReason, blackreason1, 40);
                   payBlackJnls.flag = upblackflag1;
                   payBlackJnls.upperflag = blackflag2;
                   memcpy(payBlackJnls.upblackType,"03",2);
                   memcpy(payBlackJnls.upblackKey,jnls->account,strlen(jnls->account));

                   res = DB_PayBlackJnls_add(&payBlackJnls);
                   if( res )
                   {
                           dbRollback();
                           ELOG(ERROR, "[����:%s_%s]Add���ݿ��[payBlackJnls][blackKey:%s]ʧ��,ERR:%d", session->servCode, session->sessionId, jnls->mobileno, res);
                           SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, NULL, NULL);
                           return SERVICE_EXCEPTION(SYSTEM);
                   }

                   dbCommit();
           }

           if(iRet == SQLNOTFOUND)
           {
                   upblackflag2 = 0;
                   res = DB_Sequence_read_by_name("SEQ_BLACKLIST_FLAG", (long *)&upblackflag2);
                   if(res != TTS_SUCCESS)
                   {
                       ELOG(ERROR,"[����:%s_%s]Read���ݿ�[Sequence][name:SEQ_BLACKLIST_FLAG]ʧ��, ERR:%d",
                           session->servCode, session->sessionId, res);
                       SET_EXCEPTION(session, LOCAL, SYSTEM, RUN_TO_D, NULL, NULL);
                       return SERVICE_EXCEPTION(SYSTEM);
                   }
                   memset(&payBlackListAdd, 0, sizeof(payBlackListAdd));
                   memset(&payBlackJnls, 0, sizeof(payBlackJnls));

                   memcpy(payBlackListAdd.blackKey,jnls->account, strlen(jnls->account));
                   memcpy(payBlackListAdd.blackType,"03", 2);
                   payBlackListAdd.blackLevel = 1;
                   memcpy(payBlackListAdd.blackTag,"0", 1);
                   payBlackListAdd.flag = upblackflag2;
                   payBlackListAdd.upperflag = blackflag1;


                   res = DB_PayBlackList_add(&payBlackListAdd);
                   if( res )
                   {
                           dbRollback();
                           ELOG(ERROR, "[����:%s_%s]Add���ݿ��[PayBlackList][blackKey:%s]ʧ��,ERR:%d", session->servCode, session->sessionId, jnls->account, res);
                           SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, NULL, NULL);
                           return SERVICE_EXCEPTION(SYSTEM);
                   }

                   strcpy(blackreason2,"�����ֻ��ţ�");
                   strncat(blackreason2,jnls->mobileno,11);

                   memcpy(payBlackJnls.localDate, jnls->localDate, strlen(jnls->localDate));
                   memcpy(payBlackJnls.localTime, jnls->localTime, strlen(jnls->localTime));
                   memcpy(payBlackJnls.servCode, session->servCode, strlen(session->servCode));
                   memcpy(payBlackJnls.customerId, jnls->customerId, strlen(jnls->customerId));
                   memcpy(payBlackJnls.blackKey, jnls->account, strlen(jnls->account));
                   memcpy(payBlackJnls.blackType, "03", 2);
                   payBlackJnls.blackLevel = 1;
                   memcpy(payBlackJnls.blackReason, blackreason2, 40);
                   payBlackJnls.flag = upblackflag2;
                   payBlackJnls.upperflag = blackflag1;
                   memcpy(payBlackJnls.upblackType,"01",2);
                   memcpy(payBlackJnls.upblackKey,jnls->mobileno,strlen(jnls->mobileno));

                   res = DB_PayBlackJnls_add(&payBlackJnls);
                   if( res )
                   {
                           dbRollback();
                           ELOG(ERROR, "[����:%s_%s]Add���ݿ��[payBlackJnls][blackKey:%s]ʧ��,ERR:%d", session->servCode, session->sessionId, jnls->account, res);
                           SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, NULL, NULL);
                           return SERVICE_EXCEPTION(SYSTEM);
                   }

                   dbCommit();

           }

           ELOG(ERROR, "[����:%s]�û�[mobileno:%s account:%s]�Ѽ��������", session->servCode, jnls->mobileno, jnls->account);
           SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, "31", "�������ޣ�����ϵ������");
           //SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, "31", "�û��Ѽ��������");
           return SERVICE_EXCEPTION(SYSTEM);

   }


   if( payCustomer.memo != NULL && (payCustomer.memo[0] == '1' || payCustomer.memo[0] == '2' || payCustomer.memo[0] == '5') && session->service->store == 2 )
   {
           ELOG(ERROR, "[����:%s_%s]�û�[customerId:%s]�Ѷ���", session->servCode, session->sessionId, jnls->customerId);
           SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, "31", "�û����Ƴ���");
           return SERVICE_EXCEPTION(SYSTEM);
   }

   if( payCustomer.memo != NULL && (payCustomer.memo[0] == '3' || payCustomer.memo[0] == '4')  )
   {
           ELOG(ERROR, "[����:%s_%s]�û�[customerId:%s]�Ѷ���", session->servCode, session->sessionId, jnls->customerId);
           SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, "31", "�û��Ѷ���");
           return SERVICE_EXCEPTION(SYSTEM);
   }

   memset(&payResultHandle, 0, sizeof(PayResultHandle));
//   res = DB_PayResultHandle_read_by_servCode_and_msgCode(session->servCode, session->current->headerOut->msgCode, &payResultHandle); //Ĭ��Ϊ���� ������
   res = DB_PayResultHandle_read_by_servCode_and_msgCode(session->servCode, "**", &payResultHandle); //Ĭ��Ϊ���� ������
   // update by jdw@20160401  session->current->headerOut->msgCode --->>  "**"
   if(res != TTS_SUCCESS)
   {
           ELOG(INFO, "[����:%s_%s]Read���ݿ��[PAYRESULTHANDLE][servCode:%s][MSGCODE:%s]ʧ��,ERR:%d",
                           session->servCode, session->sessionId, session->servCode, "**", res);

   }
   else
   {
           res = translateCondition(session, payResultHandle.limitCondition, conditionForReal);
           if(res != TTS_SUCCESS)
           {
                   SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, NULL, NULL);
                   return SERVICE_EXCEPTION(SYSTEM);
           }

           memset(&payResultHistory, 0, sizeof(PayResultHistory));
           res = DB_PayResultHistory_read_by_condition(conditionForReal, &payResultHistory);
           if(res != TTS_SUCCESS && res != SQLNOTFOUND)
           {
                   ELOG(ERROR, "[����:%s_%s]Read���ݿ��[PAYRESULTHISTORY][CONDITION:%s]ʧ��,ERR:%d",
                                   session->servCode, session->sessionId, conditionForReal, res);
                   SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, NULL, NULL);
                   return SERVICE_EXCEPTION(SYSTEM);
           }
           else if(res == SQLNOTFOUND)
           {
                   strncpy(payResultHistory.condition, conditionForReal, 64);
                   payResultHistory.countNum = 1;
                   res = DB_PayResultHistory_add(&payResultHistory);
                   if(res != TTS_SUCCESS)
                   {
                           ELOG(ERROR, "[����:%s_%s]Add���ݿ��[PAYRESULTHISTORY][CONDITION:%s]ʧ��,ERR:%d",
                                           session->servCode, session->sessionId, conditionForReal, res);
                           SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, NULL, NULL);
                           return SERVICE_EXCEPTION(SYSTEM);
                   }
           }
           else
           {
                   payResultHistory.countNum += 1;

                   if(payResultHistory.countNum > payResultHandle.cautionNum )
                   {
                           ELOG(ERROR, "[����:%s]�˽��׵��ս��״�������[countNum:%d|cautionNum:%d]", session->servCode, payResultHistory.countNum,payResultHandle.cautionNum );
                           SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, "31", "�˽��׵��ս��״�������");
                           return SERVICE_EXCEPTION(SYSTEM);
                   }

                   res = DB_PayResultHistory_update_by_condition(conditionForReal, &payResultHistory);
                   if(res != TTS_SUCCESS)
                   {
                           dbRollback();
                           ELOG(ERROR, "[����:%s_%s]Update���ݿ��[PAYRESULTHISTORY][CONDITION:%s]ʧ��,ERR:%d",
                                           session->servCode, session->sessionId, conditionForReal, res);
                           SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, NULL, NULL);
                           return SERVICE_EXCEPTION(SYSTEM);
                   }
           }

   	}
	#if 0
	/* �Żݽ�� add By Zhuweijin@2016-02-01 begin*/
	DataPoolGetLong(session->dataPool, TX_FAVOUR_AMT, (long *)&jnls->favourAmt);
	ELOG(INFO, "�Żݽ��: %d", jnls->favourAmt);
	/* add By Zhuweijin@2016-02-01 end*/	
	#endif

   	ELOG(INFO, "TRADECODE:%s", jnls->tradeCode);
   	if(strncmp("EXC", jnls->tradeCode, 3) == 0)
	{

		/*������������ add by Zhuweijin begin*/
		memset(&payResultHandle, 0, sizeof(PayResultHandle));
		res = DB_PayResultHandle_read_by_servCode_and_msgCode(session->servCode, "00", &payResultHandle);
		if( res !=TTS_SUCCESS && res != SQLNOTFOUND )
		{
			ELOG(ERROR, "[����:%s_%s]Read���ݿ�[PayResultHandle][servCode:%s][msgCode:00]ʧ��,ERR:%d", session->servCode, 
						session->sessionId, session->servCode, res);
			SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, NULL, NULL);
			return SERVICE_EXCEPTION(SYSTEM);
		}
		else if( res == TTS_SUCCESS )
		{
			memset(conditionForReal, 0x00, sizeof(conditionForReal));
			sprintf(conditionForReal, "00+%s+D", jnls->account);
			ELOG(INFO, "conditionForReal: %s", conditionForReal);
			memset(&payResultHistory, 0, sizeof(PayResultHistory));
			res = DB_PayResultHistory_read_by_condition(conditionForReal, &payResultHistory);
			if( res != TTS_SUCCESS && res != SQLNOTFOUND )
			{
				ELOG(ERROR, "[����:%s_%s]Read���ݿ�[PayResultHistory][condition:%s]ʧ��,ERR:%d", session->servCode, 
									session->sessionId, conditionForReal, res);
              	SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, NULL, NULL);
               	return SERVICE_EXCEPTION(SYSTEM);
           	}
           	else if( res == TTS_SUCCESS )
           	{
               	if( payResultHistory.countNum >= payResultHandle.cautionNum )
               	{
                    ELOG(ERROR, "�ÿ����ս��׳������޴���");
                    SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, "C4", "���Ŀ�ʹ�ù���Ƶ��,�������ʹ��");
                    return SERVICE_EXCEPTION(SYSTEM);
               	}
           	}

           	ELOG(DEBUG, "continue...");
        }
        /*add by Zhuwejin end*/

#if     1        /*add by jdw@20160401 for ���൥�� ���˻� ����  begin*/
		if(strncmp("EXC002", jnls->tradeCode, 6) == 0)
		{
			res = GetExc002Handle(session, session->servCode, "CU", "31", "�˽��׵��ս��״�������");    //CUSTOMERID  ��������
			if (res != TTS_SUCCESS)
				return res;
			res = GetExc002Handle(session, session->servCode, "CA", "C4", "���Ŀ�ʹ�ù���Ƶ��,�������ʹ��");   //CARD ��������
			if (res != TTS_SUCCESS)
				return res;
		}
#endif          /*add by jdw@20160401 for ���൥�� ���˻� ����  end*/

		/*�̶��̻���ѡȡ add By Zhuweijin@2015-12-11 begin*/
		res = GetFixMerchantByServCode(session);
		if(res == 0)
		{
			dataLen = 15;
			DataPoolGetString2(session->dataPool, TX_HOST_MERCHANT_ID, jnls->hostMerchId, &dataLen);
			dataLen = 8;
			DataPoolGetString2(session->dataPool, TX_HOST_TERMINAL, jnls->hostTermId, &dataLen);
			ELOG(INFO, "����[%s]�̶��̻���: %s, %s", session->servCode, jnls->hostMerchId, jnls->hostTermId);
			goto MODULES_END;
		}
		else if(res < 0)
		{
			return SERVICE_EXCEPTION(SYSTEM);
		}
		/*add By Zhuweijin@2015-12-11 end*/

        ELOG(INFO, "branchid:%s", jnls->branchId);
		//add by li.q@20150825 for �����̻��治���̻���ѯ begin
		PayUnderTakes    payUnderTakes;

		memset(&payUnderTakes, 0x00, sizeof(PayUnderTakes));

		res = DB_PayUnderTakes_read_by_branchId(jnls->branchId, &payUnderTakes);
		if(res != TTS_SUCCESS && res != SQLNOTFOUND)
		{
			ELOG(ERROR, "[����:%s_%s]Read���ݿ�[PayUnderTakes][branchId:%s]ʧ��,ERR:%d", session->servCode, 
				session->sessionId, jnls->branchId, res);
			SET_EXCEPTION(session, LOCAL, SYSTEM, RUN_TO_A, NULL, NULL);
		    return SERVICE_EXCEPTION(SYSTEM);
		}

		ELOG(INFO, "branchid[%s]itags[%s]", jnls->branchId, payUnderTakes.iTags);
		if(memcmp(payUnderTakes.iTags, "1", 1) == 0)
		{
			ELOG(INFO, "�����̻������[%s]", jnls->branchId);
			PayMerchControl payMerchControl;
			memset(&payMerchControl, 0x00, sizeof(PayMerchControl));
			res = DB_PayMerchControl_read_by_branchId_and_userId(jnls->branchId, jnls->mobileno, &payMerchControl);
			if(res != TTS_SUCCESS && res != SQLNOTFOUND)
			{
				ELOG(ERROR, "[����:%s_%s]Select���ݿ�[PayMerchControl][branchId:%s][mobileno:%s]ʧ��,ERR:%d", 
							session->servCode, session->sessionId, jnls->branchId, jnls->mobileno, res);
				SET_EXCEPTION(session, LOCAL, SYSTEM, RUN_TO_D, NULL, NULL);
				return SERVICE_EXCEPTION(SYSTEM);
			}
			else if(res == SQLNOTFOUND)
			{
				ELOG(INFO, "[����:%s_%s]Select���ݿ�[PayMerchControl][branchId:%s][mobileno:%s]ʧ��,ERR:%d", 
			                session->servCode, session->sessionId, jnls->branchId, jnls->mobileno, res);
				//SET_EXCEPTION(session, LOCAL, SYSTEM, RUN_TO_D, "03", "��Ч�̻�");
				//return SERVICE_EXCEPTION(SYSTEM);
				goto __MERCH__;
			}
			else
			{
				memcpy(jnls->hostMerchId, payMerchControl.hostMerchId, 15);
				memcpy(jnls->hostTermId, payMerchControl.hostTermId, 8);
				ELOG(INFO, "�����̻���[MERCHANTID:%s][TERMINALID:%s]", jnls->hostMerchId, jnls->hostTermId);
				DataPoolPutString(session->dataPool, TX_HOST_MERCHANT_ID, payMerchControl.hostMerchId);
				DataPoolPutString(session->dataPool, TX_HOST_TERMINAL, payMerchControl.hostTermId);
				DataPoolPutString(session->dataPool, TX_MERCHANT_NAME,  payMerchControl.hostMerchantName);
			}
		}
		//add by li.q@20150825 for �����̻��治���̻���ѯ end
		else
		{
__MERCH__:
			res = DB_TradeInfo_read_by_branchId_and_shopNo(jnls->branchId, "0000", &sTradeInfo);
			if(res != TTS_SUCCESS && res != SQLNOTFOUND)
			{
				ELOG(ERROR, "[����:%s_%s]Read���ݿ��[TRADEINFO][branchId:%s][shopNo:0000]ʧ��,ERR:%d", 
					session->servCode, session->sessionId, jnls->branchId, res);
				SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, NULL, NULL);
				return SERVICE_EXCEPTION(SYSTEM);
			}
			else if (res == SQLNOTFOUND)
			{
				res = DB_TradeInfo_read_by_branchId_and_shopNo("00000000", "0000", &sTradeInfo);
				if(res != TTS_SUCCESS)
				{
					ELOG(ERROR, "[����:%s_%s]Read���ݿ��[TRADEINFO][branchId:00000000][shopNo:0000]δ�ҵ�,ERR:%d", 
						session->servCode, session->sessionId, res);
					SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, NULL, NULL);
					return SERVICE_EXCEPTION(SYSTEM);
				}
			}
			//add by wb ԭ���̳��̻��� 20151009
			if(strncmp(session->servCode, "100059", 6) == 0)
			{
				memset(&sTradeInfo, 0, sizeof(sTradeInfo));
				res = DB_TradeInfo_read_by_branchId_and_shopNo("SER100059", "0000", &sTradeInfo);
				if(res != TTS_SUCCESS )
				{
					ELOG(ERROR, "[����:%s_%s]Read���ݿ��[TRADEINFO][branchId:%s][shopNo:0000]ʧ��,ERR:%d", 
						session->servCode, session->sessionId, "SER100059", res);
					SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, NULL, NULL);
					return SERVICE_EXCEPTION(SYSTEM);
				}
			}
			if(strncmp(session->servCode, "200052", 6) == 0 || strncmp(session->servCode, "200017", 6) == 0 || strncmp(session->servCode,"100092", 6) == 0 || strncmp(session->servCode, "200013", 6) == 0)
			{
				memset(&sTradeInfo, 0, sizeof(sTradeInfo));
				res = DB_TradeInfo_read_by_branchId_and_shopNo("SER200052", "0000", &sTradeInfo);
				if(res != TTS_SUCCESS )
				{
					ELOG(ERROR, "[����:%s_%s]Read���ݿ��[TRADEINFO][branchId:%s][shopNo:0000]ʧ��,ERR:%d", 
						session->servCode, session->sessionId, "SER200052", res);
					SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, NULL, NULL);
					return SERVICE_EXCEPTION(SYSTEM);
				}
			}
			else
			{
				//
				;
			}

			res = DB_TradeInfo_read_lock_by_branchId_and_shopNo(sTradeInfo.upBranchId, "9999", &sUpBranchInfo,&lockInfo);
			if(res != TTS_SUCCESS)
			{
				ELOG(ERROR, "[����:%s_%s]Read���ݿ��[TRADEINFO][branchId:%s][shopNo:9999]ʧ��,ERR:%d", 
					session->servCode, session->sessionId, sTradeInfo.upBranchId, res);
				SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, NULL, NULL);
				return SERVICE_EXCEPTION(SYSTEM);
			}
			
			memcpy(jnls->hostMerchId, sTradeInfo.defMerchIdDown, 15);
			memcpy(jnls->hostTermId, sTradeInfo.defTermIdDown, 8);

			PayMerchant sPayMer;

			memset(&sPayMer, 0, sizeof(PayMerchant));

			char merType[5] = {0};
			int serNo = 0 ;
			strcpy(merType,"0000"); // default pay type

			/* add by Zhuweijin@2014-12-04 begin
			* modify: ����IC���̻�����ѭ*/
			char    cardType[2] = {0};   /*������ IC��, ����*/
			dataLen = 2;
			DataPoolGetString2(session->dataPool, TX_MP_ACCOUNT_TYPE, cardType, &dataLen);
			cardType[0] = '0';  /*IC �����̻�������*/

			PayBranch payBranch2;
			memset(&payBranch2, 0, sizeof(PayBranch));
			res = DB_PayBranch_read_by_branchId(jnls->branchId, &payBranch2);
			if(res)
			{
				DB_TradeInfo_free_lock(&lockInfo);
				ELOG(ERROR,"[����:%s_%s]Read���ݿ��[paybranch][branchId:%s]ʧ��,ERR:%d", session->servCode, 
					session->sessionId, jnls->branchId, res);
				SET_EXCEPTION(session, LOCAL, SYSTEM, RUN_TO_D, "D3", "����������");
				return SERVICE_EXCEPTION(SYSTEM);
			}

			/*֧����ʽA*/
			if( (strncmp(session->servCode, "100034", 6) == 0 && (strcmp("0000000000", jnls->productId) == 0 || 
				strcmp("0000000001", jnls->productId) == 0))|| strncmp(session->servCode, "100086", 6) == 0 || 
				(strcmp(session->servCode, "100080") == 0 && payBranch2.branchAttr[1] == '1') || 
				strncmp(session->servCode, "200038", 6) == 0 || strncmp(session->servCode, "200052", 6) == 0 ||
				strncmp(session->servCode, "200017", 6) == 0 || strncmp(session->servCode, "200013", 6) == 0) //update by jdw@20160323 for 200052 �����׿�
			{
				strcpy(merType,"0001");
				serNo = sUpBranchInfo.serNoA;
				dataLen = 22;
				DataPoolGetString2(session->dataPool, ACC_TXSHOPNO, shopNo, &dataLen);

				ELOG(INFO, "SHOPNO:%s", shopNo);

				PayBranch payBranch;
				memset(&payBranch, 0, sizeof(PayBranch));

				res = DB_PayBranch_read_by_branchId(jnls->branchId, &payBranch);
				if(res)
				{
					DB_TradeInfo_free_lock(&lockInfo);
					ELOG(ERROR,"[����:%s_%s]Read���ݿ��[paybranch][branchId:%s]ʧ��,ERR:%d", session->servCode, 
						session->sessionId, jnls->branchId, res);
					SET_EXCEPTION(session, LOCAL, SYSTEM, RUN_TO_D, "D3", "����������");
					return SERVICE_EXCEPTION(SYSTEM);
				}

				/*֧����ʽA�׿� add by wpl@20150523 start*/
				int randnum;
				PayDeduct paydeduct;
				Select_Info sInfo;
				memset(&sInfo, 0x00, sizeof(Select_Info));
				res = DB_PayDeduct_open_select_by_branchid_and_flag(jnls->branchId,0,&sInfo);
				if(res != TTS_SUCCESS)
				{
					ELOG(ERROR,"[����:%s_%s]OpenSelect���ݿ��[PayDeduct][branchid:%s][flag:0]ʧ��,ERR:%d", 
						session->servCode, session->sessionId, jnls->branchId, res);
					DB_TradeInfo_free_lock(&lockInfo);
					SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, NULL, NULL);
					return SERVICE_EXCEPTION(SYSTEM);
				}
				while(1)
				{
					memset(&paydeduct,0,sizeof(paydeduct));
					res = DB_PayDeduct_fetch_select(&sInfo,&paydeduct);
					if(res != TTS_SUCCESS && res != SQLNOTFOUND)
					{
						ELOG(ERROR, "[����:%s_%s]Fetch���ݿ�[PayDeduct]ʧ��, ERR:%d", session->servCode, session->sessionId, res);
						DB_TradeInfo_free_lock(&lockInfo);
						SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, NULL, NULL);
						return SERVICE_EXCEPTION(SYSTEM);
					}
					else if(res == SQLNOTFOUND)
					{
						ELOG(INFO,"�� PayDeduct ��Ĭ������ !");
						DB_PayDeduct_close_select(&sInfo);

						res = DB_PayDeduct_open_select_by_branchid_and_flag("00000000",0,&sInfo);
						if(res != TTS_SUCCESS)
						{
							ELOG(ERROR,"����:%s_%s]Open���ݿ�[PayDeduct][branchid:00000000][flag:0]ʧ��,ERR:%d", 
								session->servCode, session->sessionId, res);
							DB_TradeInfo_free_lock(&lockInfo);
							SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, NULL, NULL);
							return SERVICE_EXCEPTION(SYSTEM);
						}

						while(1)
						{
							res = DB_PayDeduct_fetch_select(&sInfo,&paydeduct);
							if(res != TTS_SUCCESS && res != SQLNOTFOUND)
							{
								ELOG(ERROR, "[����:%s_%s]Fetch���ݿ�[PayDeduct]ʧ��, ERR:%d", session->servCode, 
									session->sessionId, res);
								DB_TradeInfo_free_lock(&lockInfo);
								SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, NULL, NULL);
								return SERVICE_EXCEPTION(SYSTEM);
							}	
							else if(res == SQLNOTFOUND)
							{
								ELOG(INFO,"����δ����Ĭ�ϸ��� !");
								break;
							}
							if(jnls->amount > paydeduct.lowamount && jnls->amount <= paydeduct.upamount)
							{
								ELOG(INFO,"�� paydeduct �����ҵ�Ĭ������ !");
								srand( (unsigned)time( NULL ) );
								randnum = rand() % 100;

								if( randnum + 1 <= paydeduct.probability )
								{
									if(cardType[0] == '0')
									{
										strcpy(merType,"0002");
										serNo = sUpBranchInfo.serNoB;
									} 
								}
								else
								{
									if(cardType[0] == '0')
									{
										strcpy(merType,"0001");
										serNo = sUpBranchInfo.serNoA;
									}
								}  


								if(shopNo[0] == '1' && payBranch.branchAttr[1] == '0')
								{
									if(cardType[0] == '0')
									{
										strcpy(merType,"0002");
										serNo = sUpBranchInfo.serNoB;
									}
								}
								break;
							}
						} 
						break;                   
					}

					if(jnls->amount > paydeduct.lowamount && jnls->amount <= paydeduct.upamount)
					{
						ELOG(INFO,"�� paydeduct �����ҵ���Ӧ���������� !");
						srand( (unsigned)time( NULL ) );
						randnum = rand() % 100;

						if( randnum + 1 <= paydeduct.probability )
						{
							if(cardType[0] == '0')
							{
								strcpy(merType,"0002");
								serNo = sUpBranchInfo.serNoB;
							}
						}
						else
						{
							if(cardType[0] == '0')
							{
								strcpy(merType,"0001");
								serNo = sUpBranchInfo.serNoA;
							}
						}

						if(shopNo[0] == '1' && payBranch.branchAttr[1] == '0')
						{
							if(cardType[0] == '0')
							{
								strcpy(merType,"0002");
								serNo = sUpBranchInfo.serNoB;
							}
						}
						break;
					}
				}           
				DB_PayDeduct_close_select(&sInfo);
				/*֧����ʽA�׿� add by wpl@20150523 end*/
			}
			
				//modify by li.q@20150313 for ��Ʒ����
#if 0
			PayBranch payBranch2;
			memset(&payBranch2, 0, sizeof(PayBranch));

			res = DB_PayBranch_read_by_branchId(jnls->branchId, &payBranch2);
			if(res)
			{
				DB_TradeInfo_free_lock(&lockInfo);
				ELOG(ERROR,"��ȡpaybranchʧ��(%d)",res);
				SET_EXCEPTION(session, LOCAL, SYSTEM, RUN_TO_D, "D3", "����������");
				return SERVICE_EXCEPTION(SYSTEM);
			}
#endif
				/*֧����ʽB*/
				if( (strncmp(session->servCode, "100034", 6) == 0 && strcmp("0000000002", jnls->productId) == 0)|| 
					(strcmp(session->servCode, "100087")==0 && (payBranch2.branchAttr[1]=='1' || payBranch2.branchAttr[1] == '2'))|| 
					(strcmp(session->servCode, "100080") == 0 && payBranch2.branchAttr[1] == '2') || strncmp(session->servCode,"100092",6)==0)
				{
					/*�ⶥ�׿��̻�ת�� add by Zhuweijin @20141112 begin*/
#if 0
					if(jnls->amount < sTradeInfo.amountA)
					{
						strcpy(merType,"0001");
						serNo = sUpBranchInfo.serNoA;
						//sUpBranchInfo.serNoA += 1;  modify by Zhuweijin
					}
					else
					{
						strcpy(merType,"0002");
						serNo = sUpBranchInfo.serNoB;
						//sUpBranchInfo.serNoB += 1;  modify by Zhuweijin
					}
#endif
					/*���׽����������, ���ܾ����״��� add by Zhuweijin@2014-12-22 begin*/
					int     rejChance;
					if( jnls->amount > sTradeInfo.amountRej )
					{
						srand( (unsigned)time(NULL) );
						rejChance = rand() % 100;
						ELOG(INFO, "���ܾ�����: %d  %d\n", rejChance, sTradeInfo.chanceRej);
						if( rejChance < sTradeInfo.chanceRej )
						{
							DB_TradeInfo_free_lock(&lockInfo);   /* add ע�ͷ��α� */
							ELOG(ERROR, "�����������,��ֹ����");
							SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_A, "C4", "���׽�����������");
							return SERVICE_EXCEPTION(SYSTEM);
						}
					}
					/*add by Zhuweijin@2014-12-22 end*/

					if(sTradeInfo.amountB == 0)
					{
						if(cardType[0] == '0')
						{
							strcpy(merType, "0002");
							serNo = sUpBranchInfo.serNoB;
						}
					}
					else
					{
						if( jnls->amount < sTradeInfo.amountB )
						{
							strcpy(merType, "0001");
							serNo = sUpBranchInfo.serNoA;
						}
						else
						{
							strcpy(merType, "0002");
							serNo = sUpBranchInfo.serNoB;
						}
					}
					//add by Zhuweijin end
				}

				if(strncmp(session->servCode, "100074", 6) == 0  || strcmp(session->servCode, "100088") == 0)
				{
					if(cardType[0] == '0')
					{
						strcpy(merType,"0003");
						serNo = sUpBranchInfo.serNoC;
						//sUpBranchInfo.serNoC += 1;  modify by Zhuweijin
					}
				}

				if(strncmp(session->servCode, "100079", 6) == 0  || strcmp(session->servCode, "100089") == 0)
				{
					if(cardType[0] == '0')
					{
						strcpy(merType,"0004");
						serNo = sUpBranchInfo.serNoD;
						//sUpBranchInfo.serNoD += 1;  modify by Zhuweijin
					}
				}

				//add by wb_20150330 ��ǿ����� begein                                                               
				if( strncmp(session->servCode, "200032", 6) == 0 || strncmp(session->servCode, "200035", 6) == 0)
				{                                                                                                        
					strcpy(merType, "0000");
					//goto END;                                                                                           
				}    
				//add by wb_20150330 ��ǿ����� end
        		//add by wb@20150901 �̳�֧�� begin
        		if( strncmp(session->servCode, "200053", 6) == 0 || strncmp(session->servCode, "200054", 6) == 0)
        		{
           			if( jnls->amount <= sUpBranchInfo.amountA )
           			{
                        strcpy(merType,"0001");
                        serNo = sUpBranchInfo.serNoA;
           			}
           			else if( jnls->amount > sUpBranchInfo.amountA )
           			{
                      	strcpy(merType, "0002");
                      	serNo = sUpBranchInfo.serNoB;
           			}
        		}
        		//by wb end
				if(strncmp(session->servCode, "100059", 6) == 0)  //update by jdw@20160323 for 200052 �����׿�
				{
					strcpy(merType, "0001");
					serNo = sUpBranchInfo.serNoA;
				}
				/* ������� */
				PayTimeCtrl   payTimeCtrl;
				memset(&payTimeCtrl, 0x00, sizeof(PayTimeCtrl));
				res = DB_PayTimeCtrl_read_by_servCode_and_branchId(session->servCode, jnls->branchId, &payTimeCtrl);
				if( res != TTS_SUCCESS && res != SQLNOTFOUND )
				{
					ELOG(ERROR, "[����:%s_%s]Read���ݿ�[PayTimeCtrl][servCode: %s][branchid:%s]ʧ��,ERR:%d", \
										session->servCode, session->sessionId, session->servCode, jnls->branchId, res);
					SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_A, NULL, NULL);
					DB_TradeInfo_free_lock(&lockInfo);   /* add ע�ͷ��α� */
					return SERVICE_EXCEPTION(SYSTEM);
				}
				else if( res == SQLNOTFOUND )
				{
					ELOG(INFO, "����(%s)δ���ý���(%s)����", jnls->branchId, session->servCode);
					res = DB_PayTimeCtrl_read_by_servCode_and_branchId(session->servCode, "00000000", &payTimeCtrl);
					if( res != TTS_SUCCESS && res != SQLNOTFOUND )
					{
						ELOG(INFO, "[����:%s_%s]Read���ݿ�[PayTimeCtrl][servCode: %s][branchid:00000000]ʧ��,ERR:%d", 
							session->servCode, session->sessionId, session->servCode, res);
					}
					else if( res == SQLNOTFOUND )
					{
						ELOG(INFO, "[����:%s_%s]Read���ݿ�[PayTimeCtrl][servCode: %s][branchid:00000000]δ�ҵ�,ERR:%d", 
							session->servCode, session->sessionId, session->servCode, res);
						payTimeCtrl.tmCardCtrl = 0;
					}
				}
				ELOG(INFO, "MERTYPE[%s]", merType);
				if(strcmp(merType,"0000") != 0 )
				{
					/*�̻����滻����  add by Zhuweijin@2014-10-23 begin */
					PayCardCtrl   payCardCtrl;
					time_t        times;
					time(&times);
					//int flood = 0;

					/* ���зⶥ����ͨ������ add by wb@20150730 begin*/
					memset(&payCardTradeCtrl, 0, sizeof(payCardTradeCtrl));
					res = DB_PayCardTradeCtrl_read_by_bankId_and_issUsers(cardBin.bankId, cardBin.issusers, &payCardTradeCtrl);
					if( res != TTS_SUCCESS && res != SQLNOTFOUND )
					{
						ELOG(ERROR,"[����:%s_%s]Read���ݿ�[PayCardTradeCtrl][bankId:%s][issUsers:%s][cardBin:%s]ʧ��,ERR:%d",
 							session->servCode, session->sessionId, cardBin.bankId, cardBin.issusers, cardBin.cardBin, res);
						SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_A, NULL, NULL);
						return SERVICE_EXCEPTION(SYSTEM);
					}
					else if( res == SQLNOTFOUND )
					{
						ELOG(INFO, "δ���ÿ�BIN��������");
					}

	            	/* add by wb@20150730 end*/   
					DataPoolPutString(session->dataPool, TX_PRE_BRANCH_ID, sUpBranchInfo.branchId);

					memset(&payCardCtrl, 0x00, sizeof(PayCardCtrl));
					res = DB_PayCardCtrl_read_by_cardNo(jnls->account, &payCardCtrl);
					if( res != TTS_SUCCESS && res != SQLNOTFOUND )
					{
						DB_TradeInfo_free_lock(&lockInfo);   /* add ע�ͷ��α� */
						ELOG(ERROR, "[����:%s_%s]Read���ݿ�[PayCardCtrl][cardNo:%s]ʧ��,ERR:%d", 
							session->servCode, session->sessionId, jnls->account, res);
						SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_A, NULL, NULL);
						return SERVICE_EXCEPTION(SYSTEM);
					}
					else if( res == TTS_SUCCESS )
					{
						ELOG(INFO, "ʱ���:%d, ����ʱ��:%d", times-payCardCtrl.timeStamp, payTimeCtrl.tmCardCtrl);
						if( strcmp("00", payCardCtrl.msgCode) == 0 )
						{
							if( times - payCardCtrl.timeStamp <= payTimeCtrl.tmCardCtrl )
							{
								DB_TradeInfo_free_lock(&lockInfo);   /* add ע�ͷ��α� */
								ELOG(ERROR, "��λʱ����, ���״�������");
								SET_EXCEPTION(session, LOCAL, SYSTEM, RUN_TO_A, "C4", "�˿�ʹ��Ƶ�ʹ���,���Ժ�����");
								return SERVICE_EXCEPTION(SYSTEM);
							}
							#if 0 
							/*���ƻ��̻����� add By Zhuweijin@2015-11-19 begin*/
							else if(strcmp(payCardCtrl.merchTag, "DZ") == 0)
							{
								ELOG(INFO, "�����ϱʽ����̻���(���ƻ�): %s", payCardCtrl.merchantId);
								DB_TradeInfo_free_lock(&lockInfo);
								strncpy( jnls->hostMerchId, payCardCtrl.merchantId, 15);
                                strncpy( jnls->hostTermId, payCardCtrl.termId, 8);
                                DataPoolPutString(session->dataPool, TX_MERCHANT_NAME, payCardCtrl.merchName);
								goto __END__;
							} /*add By Zhuweijin@2015-11-19 end*/
							#endif
						}
						else
						{
							if( strcmp(merType, payCardCtrl.payType) == 0 )
							{
								if( times - payCardCtrl.timeStamp <= payTimeCtrl.tmCardCtrl )
								{
									/*�̻��޶��ж�,��ֹ���� add by Zhuweijin@20150529 begin*/
									memset(&sPayMer, 0x00, sizeof(PayMerchant));
									res = DB_PayMerchant_read_by_branchId_and_merchantId_and_termId(sUpBranchInfo.branchId, \
												payCardCtrl.merchantId, payCardCtrl.termId , &sPayMer);
									if( res )
									{
										DB_TradeInfo_free_lock(&lockInfo);
										ELOG(ERROR, "[����:%s_%s]Read���ݿ�[PayMerchant][branchId:%s][merchantId:%s]ʧ��,ERR:%d", 
										session->servCode, session->sessionId, sUpBranchInfo.branchId, payCardCtrl.merchantId, res);
										SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_A, NULL, NULL);
										return SERVICE_EXCEPTION(SYSTEM);
									}
									if( sPayMer.mlevel == 1 && sPayMer.DayAmount + jnls->amount > sPayMer.DayUpLimit )
									{
										DB_TradeInfo_free_lock(&lockInfo);
										ELOG(ERROR, "���̻���ȳ���");
										SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_A, "D4", "�˿�ʹ��Ƶ�ʹ���,���Ժ�����");
										return SERVICE_EXCEPTION(SYSTEM);
									}
									else if( sPayMer.mlevel == 0 && ( ( sPayMer.upLimit > 0 && jnls->amount > sPayMer.upLimit ) 
											|| ( sPayMer.DayAmount > 0 && sPayMer.DayAmount + jnls->amount > sPayMer.DayUpLimit ) ) )
									{
										DB_TradeInfo_free_lock(&lockInfo);
										ELOG(ERROR, "���̻���ȳ���");
										SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_A, "D4", "�˿�ʹ��Ƶ�ʹ���,���Ժ�����");
										return SERVICE_EXCEPTION(SYSTEM);
									}
									/*add by Zhuweijin@20150529 end*/
									strncpy( jnls->hostMerchId, payCardCtrl.merchantId, 15);
									strncpy( jnls->hostTermId, payCardCtrl.termId, 8);     

									/*add by Zhuweijin for using orig merchant name*/
									DataPoolPutString(session->dataPool, TX_MERCHANT_NAME, payCardCtrl.merchName);
									ELOG(INFO, "�����������̻���: [%s][MsgCode:%s][merType:%s]", payCardCtrl.merchantId, 
											payCardCtrl.msgCode, payCardCtrl.payType);                       
									DB_TradeInfo_free_lock(&lockInfo);
									goto __END__;
								}
							}
							else
							{
								if( times - payCardCtrl.timeStamp <= payTimeCtrl.tmCardCtrl )
								{
									DB_TradeInfo_free_lock(&lockInfo);   /* add ע�ͷ��α� */
									ELOG(ERROR, "�涨ʱ����, ʧ�ܽ��׸���֧����ʽ(%s -->%s)", payCardCtrl.payType, merType);
									SET_EXCEPTION(session, LOCAL, SYSTEM, RUN_TO_A, "C4", "�˿�ʹ��Ƶ�ʹ���,���Ժ�����");
									return SERVICE_EXCEPTION(SYSTEM);
								}
							}
						}
					}

					ELOG(INFO, "�̻����滻");
					Select_Info   sResInfo;

					/*merState : 1-������ 0-������ ����: ����ʹ��*/
					/*���ʽ��������ж� modify by Zhuweijin@2014-12-16 begin*/
					/*�����̻���ѭ add by Zhuweijin@2014-12-31*/
					int     found = 0;
					int		yxSerNo = 0;
					char	txnFlag[3] = {0};
					/* ��������,�̻����� add by Zhuweijin@2015-6-15 begin*/
					char	merState[2] = {0};

					dataLen = 2;
					DataPoolGetString2(session->dataPool, TX_CARD_TYPE2, txnFlag, &dataLen);		
					
					ELOG(INFO, "��ѵ�̻���Ϣ:branchid[%s] mertype[%s] serno[%d],cardFlag:[%s]", 
						sUpBranchInfo.branchId, merType, serNo, txnFlag);
					
					/*�������̻�ѡ�� add By Zhuweijin@2015-11-09 begin*/
					res = GetMerchantIdByLocal(session, jnls, sUpBranchInfo.branchId, merType, &serNo);
					if(res < 0)
					{
						DB_TradeInfo_free_lock(&lockInfo);
						return SERVICE_EXCEPTION(SYSTEM);
					}
					else if(res == 0)
					{
						memset(&sPayMer, 0x00, sizeof(PayMerchant));
						sPayMer.serNo = serNo;
						goto __MERCHANT_CYCLE_END;
					}
					/*�������̻�ѡ�� add By Zhuweijin@2015-11-09 end*/
					if ( strcmp(txnFlag, "12") == 0 )
					{
						merState[0] = '0';    /* ֻ���������Ƶ��̻� */
					}
					else
					{
						merState[0] = '1'; 
					}
					/* add by Zhuweijin@2015-06-15 end */
					
					/*���ƻ��̻�ѡ�� add By Zhuweijin@2016-11-16 begin*/
					int		level = 0;
					level = atol(payCardTradeCtrl.cardTag);
					ELOG(INFO, "level: %d", level);
					if( (strcmp(merType, "0003") == 0 || strcmp(merType, "0004") == 0) && level > 1)
					{
						res = GetSpecializeMerchantId(session, jnls, merType, merState, level, &serNo);
						if(res < 0)
						{
							DB_TradeInfo_free_lock(&lockInfo);
							return SERVICE_EXCEPTION(SYSTEM);
						}
						else if(res == 0)
						{
							memset(&sPayMer, 0x00, sizeof(PayMerchant));
							sPayMer.serNo = serNo;
							goto __MERCHANT_CYCLE_END;
						}
					}
					/*���ƻ��̻�ѡ�� add By Zhuweijin@2015-11-16 End*/

					memset(&sResInfo, 0x00, sizeof(Select_Info));
					res = DB_PayMerchant_open_select_by_branchId_and_merType_and_mlevel_and_merState_LE_and_serNo_GE_order_by_serNo(sUpBranchInfo.branchId, merType, 1, merState, serNo, &sResInfo);
					if( res )
					{
						DB_TradeInfo_free_lock(&lockInfo);   /* add ע�ͷ��α� */
						ELOG(ERROR, "[����:%s_%s]Open���ݿ�[PayMerchant][branchId:%s][merType:%s][mlevel:1][merState:%s]\
							[serNo:%d]ʧ��,ERR:%d", session->servCode, session->sessionId, sUpBranchInfo.branchId, merType, 
							merState, serNo, res);
						SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_A, NULL, NULL);
						return SERVICE_EXCEPTION(SYSTEM); 
					}
					while(1)
					{
						memset(&sPayMer, 0x00, sizeof(PayMerchant));
						res = DB_PayMerchant_fetch_select(&sResInfo, &sPayMer);
						if( res != TTS_SUCCESS && res != SQLNOTFOUND )
						{
							DB_TradeInfo_free_lock(&lockInfo);   /* add ע�ͷ��α� */
							ELOG(ERROR, "[����:%s_%s]Fetch���ݿ�[PayMerchant]ʧ��,ERR:%d", session->servCode, 
								session->sessionId, res);
							DB_PayMerchant_close_select(&sResInfo);
							SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_A, NULL, NULL);
							return SERVICE_EXCEPTION(SYSTEM);
						}
						else if( res == SQLNOTFOUND )
						{
							/*ȷ���̻�������ѭ��� add begin*/
							DB_PayMerchant_close_select(&sResInfo);
							yxSerNo = 1;
							memset(&sResInfo, 0x00, sizeof(Select_Info));
							ELOG(INFO,"�����̻���1��ʼ��ѭ");
							res = DB_PayMerchant_open_select_by_branchId_and_merType_and_mlevel_and_merState_LE_and_serNo_GE_order_by_serNo(sUpBranchInfo.branchId, merType, 1, merState, yxSerNo, &sResInfo);
							if( res )
							{
								DB_TradeInfo_free_lock(&lockInfo); 
								ELOG(ERROR, "[����:%s_%s]Open���ݿ�[PayMerchant][BRANCHID:%s][MERTYPE:%s][MLEVEL:1]\
									[MERSTATE:%s][SERNO:%d]ʧ��,ERR:%d", session->servCode, session->sessionId, 
									sUpBranchInfo.branchId, merType, merState, yxSerNo,res);
								SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_A, NULL, NULL);
								return SERVICE_EXCEPTION(SYSTEM);
							}
							while(1)
							{
								memset(&sPayMer, 0x00, sizeof(PayMerchant));
								res = DB_PayMerchant_fetch_select(&sResInfo, &sPayMer);
								if( res != TTS_SUCCESS && res != SQLNOTFOUND )
								{
									DB_TradeInfo_free_lock(&lockInfo);   /* add ע�ͷ��α� */
									ELOG(ERROR, "[����:%s_%s]Fetch���ݿ�[PayMerchant][BRANCHID:%s][MERTYPE:%s][MLEVEL:%d]\
										[MERSTATE:%s][SERNO<%d]ʧ��, ERR:%d", session->servCode, session->sessionId, 
										sUpBranchInfo.branchId, merType, 1, merState, yxSerNo, res);
									DB_PayMerchant_close_select(&sResInfo);
									SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_A, NULL, NULL);
									return SERVICE_EXCEPTION(SYSTEM);
								}
								else if( res == SQLNOTFOUND )
								{
									ELOG(INFO, "[����:%s]û���������������������������̻���", session->servCode);
									DB_PayMerchant_close_select(&sResInfo);
									goto __NEXT__;
								}
								if( jnls->amount > sPayMer.upLimit )
								{
									//ELOG(ERROR, "�����̻�: ���ʽ���(%d  %d)", jnls->amount, sPayMer.upLimit);
									continue;
								}
								if( sPayMer.DayAmount + jnls->amount  <= sPayMer.DayUpLimit )
								{
									//sPayMer.DayAmount += jnls->amount;
									memset(&catag, 0, sizeof(catag));
									strncpy(catag, sPayMer.merchantId, 1);
									if( (strcmp(merType, "0001") == 0 && strstr(payCardTradeCtrl.chanceA, catag) != NULL) ||
										(strcmp(merType, "0002") == 0 && strstr(payCardTradeCtrl.chanceB, catag) != NULL) || 
										(strcmp(merType, "0003") == 0 && strstr(payCardTradeCtrl.chanceC, catag) != NULL) || 
										(strcmp(merType, "0004") == 0 && strstr(payCardTradeCtrl.chanceD, catag) != NULL) )
									{  
										continue;
									}

									ELOG(INFO, "�����̻���: %s %s", sPayMer.merchantId, sPayMer.termId);
									DB_PayMerchant_close_select(&sResInfo);
									found = 1;
									goto __NEXT__;
								}
							}
										/*add end*/
						}

						if( jnls->amount > sPayMer.upLimit )
						{
							//ELOG(ERROR, "�����̻�: ���ʽ���(%d  %d)", jnls->amount, sPayMer.upLimit);
							continue;
						}
						memset(&catag, 0, sizeof(catag));
						strncpy(catag, sPayMer.merchantId, 1);
						if(	(strcmp(merType, "0001") == 0 && strstr(payCardTradeCtrl.chanceA, catag) != NULL) ||
							(strcmp(merType, "0002") == 0 && strstr(payCardTradeCtrl.chanceB, catag) != NULL) ||
							(strcmp(merType, "0003") == 0 && strstr(payCardTradeCtrl.chanceC, catag) != NULL) || 
							(strcmp(merType, "0004") == 0 && strstr(payCardTradeCtrl.chanceD, catag) != NULL) )
						{  
							continue;
						}
						/* add by wb end*/
						sPayMer.DayAmount += jnls->amount;
						if( sPayMer.DayAmount  <= sPayMer.DayUpLimit )
						{
							//sPayMer.DayAmount += jnls->amount;
							ELOG(INFO, "�����̻���: %s %s", sPayMer.merchantId, sPayMer.termId);
							DB_PayMerchant_close_select(&sResInfo);
							found = 1;
							break;
						}

					}

__NEXT__:
					if(found != 1)
					{
						ELOG(INFO, "��ͨ�̻�����ѭ");
						memset(&sResInfo, 0x00, sizeof(Select_Info));
						res = DB_PayMerchant_open_select_by_branchId_and_merType_and_mlevel_and_merState_LE_and_serNo_GE_order_by_serNo(sUpBranchInfo.branchId, merType, 0, merState, serNo, &sResInfo);
						if( res )
						{
							DB_TradeInfo_free_lock(&lockInfo);   /* add ע�ͷ��α� */
							ELOG(ERROR, "[����:%s_%s]OpenSelect���ݿ�[PayMerchant][branchId:%s][merType:%s][mlevel:0]\
								[merState:%s][serNo:%d]ʧ��, ERR:%d", session->servCode, session->sessionId, 
								sUpBranchInfo.branchId, merType, merState, serNo, res);
							SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_A, NULL, NULL);
							return SERVICE_EXCEPTION(SYSTEM);
						}
						while(1)
						{
							memset(&sPayMer, 0x00, sizeof(PayMerchant));
							res = DB_PayMerchant_fetch_select(&sResInfo, &sPayMer);
							if( res != TTS_SUCCESS && res != SQLNOTFOUND )
							{
								DB_TradeInfo_free_lock(&lockInfo);   /* add ע�ͷ��α� */
								ELOG(ERROR, "[����:%s_%s]Fetch���ݿ�[PayMerchant]ʧ��,ERR:%d", session->servCode, 
									session->sessionId, res);
								DB_PayMerchant_close_select(&sResInfo);
								SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_A, NULL, NULL);
								return SERVICE_EXCEPTION(SYSTEM);
							}
							if( res == SQLNOTFOUND )
							{
								DB_PayMerchant_close_select(&sResInfo);
								ELOG(INFO, "�ֻ���1��ʼ");
								serNo = 1;
								memset(&sResInfo, 0x00, sizeof(Select_Info));
								res = DB_PayMerchant_open_select_by_branchId_and_merType_and_mlevel_and_merState_LE_and_serNo_GE_order_by_serNo(sUpBranchInfo.branchId, merType, 0, merState, serNo, &sResInfo );
								if( res )
								{
									DB_TradeInfo_free_lock(&lockInfo);   /* add ע�ͷ��α� */
									ELOG(ERROR, "[����:%s_%s]Open���ݿ�[PayMerchant][branchId:%s][merType:%s][mlevel:0]\
										[merState:%s][serNo:%d]ʧ��, ERR:%d", session->servCode, session->sessionId, 
										sUpBranchInfo.branchId, merType, merState, serNo, res);
									SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_A, NULL, NULL);
									return SERVICE_EXCEPTION(SYSTEM);
								}
								while(1)
								{
									memset(&sPayMer, 0x00, sizeof(PayMerchant));
									res = DB_PayMerchant_fetch_select(&sResInfo, &sPayMer);
									if( res != TTS_SUCCESS && res != SQLNOTFOUND )
									{
										DB_TradeInfo_free_lock(&lockInfo);   /* add ע�ͷ��α� */
										ELOG(ERROR, "[����:%s_%s]Fetch���ݿ�[PayMerchant]ʧ��, ERR:%d", 
										session->servCode, session->sessionId, res);
										DB_PayMerchant_close_select(&sResInfo);
										SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_A, NULL, NULL);
										return SERVICE_EXCEPTION(SYSTEM);
									}
									if( res == SQLNOTFOUND )
									{
										DB_TradeInfo_free_lock(&lockInfo);   /* add ע�ͷ��α� */
										ELOG(INFO, "û���ҵ�Ҫ����̻��ź��ն˺�");
										DB_PayMerchant_close_select(&sResInfo);
										if ( strcmp(txnFlag, "12") == 0 )
										{
											SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_A, "D3", "IC������,��忨");
										}
										else
										{
											SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_A, NULL, NULL);
										}
										return SERVICE_EXCEPTION(SYSTEM);
									}
									memset(&catag, 0, sizeof(catag));
									strncpy(catag, sPayMer.merchantId, 1);
									if( (strcmp(merType, "0001") == 0 && strstr(payCardTradeCtrl.chanceA, catag) != NULL) ||
										(strcmp(merType, "0002") == 0 && strstr(payCardTradeCtrl.chanceB, catag) != NULL) || 
										(strcmp(merType, "0003") == 0 && strstr(payCardTradeCtrl.chanceC, catag) != NULL) || 
										(strcmp(merType, "0004") == 0 && strstr(payCardTradeCtrl.chanceD, catag) != NULL) )
									{  
										continue;
									}
									// add by wb end
									/*�����޶�*/
									if(sPayMer.upLimit > 0 && jnls->amount <= sPayMer.upLimit)
									{
										if((sPayMer.DayAmount + jnls->amount <= sPayMer.DayUpLimit) || sPayMer.DayUpLimit == 0)
										{
											DB_PayMerchant_close_select(&sResInfo);
											break;
										}
									}
									else if( sPayMer.upLimit == 0 )
									{
										if ((sPayMer.DayAmount + jnls->amount <= sPayMer.DayUpLimit) || sPayMer.DayUpLimit == 0)
										{
											DB_PayMerchant_close_select(&sResInfo);
											break;
										}
									}

								}
							}
							memset(&catag, 0, sizeof(catag));
							strncpy(catag, sPayMer.merchantId, 1);
							if( (strcmp(merType, "0001") == 0 && strstr(payCardTradeCtrl.chanceA, catag) != NULL) ||
								(strcmp(merType, "0002") == 0 && strstr(payCardTradeCtrl.chanceB, catag) != NULL) || 
								(strcmp(merType, "0003") == 0 && strstr(payCardTradeCtrl.chanceC, catag) != NULL) || 
								(strcmp(merType, "0004") == 0 && strstr(payCardTradeCtrl.chanceD, catag) != NULL) )
							{  
								continue;
							}
							/* add by wb end*/
							/*�޶��ж�*/
							if( sPayMer.upLimit != 0 && sPayMer.upLimit >= jnls->amount )
							{
								ELOG(INFO, "�޶��̻���");
								if((sPayMer.DayAmount + jnls->amount <= sPayMer.DayUpLimit) || sPayMer.DayUpLimit == 0)
								{
									DB_PayMerchant_close_select(&sResInfo); 
									break;
								}
							}
							else if( sPayMer.upLimit == 0 )
							{
								ELOG(INFO, "���޶��̻���");
								if ((sPayMer.DayAmount + jnls->amount <= sPayMer.DayUpLimit) || sPayMer.DayUpLimit == 0)
								{
									DB_PayMerchant_close_select(&sResInfo); 
									break;
								}
							}
						}
						/*�����޶��̻��� modify by Zhuweijin@2014-12-16 end*/
					} /*add by Zhuweijin �����̻�����ѯ�� */

					memcpy(jnls->hostMerchId, sPayMer.merchantId, 15);
					memcpy(jnls->hostTermId, sPayMer.termId, 8);
					DataPoolPutString(session->dataPool, TX_MERCHANT_NAME,  sPayMer.merchantName);

            		if(strncmp(sPayMer.merchantId, "000000000000000", 15) == 0 && strncmp(session->servCode, "200053", 6) == 0)
            		{
                    	ELOG(ERROR,"[����:%s]���׽��[amount:%d]����[upbranch.amountA:%d]", session->servCode, 
								jnls->amount, sUpBranchInfo.amountA);
						DB_TradeInfo_free_lock(&lockInfo);
						SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, "61", "���׽���");
						return SERVICE_EXCEPTION(SYSTEM);
            		}
					DataPoolPutString(session->dataPool, TX_PROVINCE_NAME2, sPayMer.province);
					ELOG(INFO, "��ȡ�̻���[%s][%s][%d][%s]", jnls->hostMerchId, jnls->hostTermId, sPayMer.serNo, 
						sPayMer.merchantName);

__MERCHANT_CYCLE_END:
					if( strncmp(merType, "0001", 4) == 0 )
					{
						sUpBranchInfo.serNoA = sPayMer.serNo+1;
					}
					if( strncmp(merType, "0002", 4) == 0 )
					{
						sUpBranchInfo.serNoB = sPayMer.serNo+1;
					}
					if( strncmp(merType, "0003", 4) == 0 )
					{
						sUpBranchInfo.serNoC = sPayMer.serNo+1;
					}
					if( strncmp(merType, "0004", 4) == 0 )
					{
						sUpBranchInfo.serNoD = sPayMer.serNo+1;
					}
					/*add by Zhuweijin@2014-10-23 end*/
					/*modify by Zhuweijin@2014-12-04 end*/

					if(strcmp("EXC003",jnls->tradeCode) == 0)
					{
						res = DB_TradeInfo_update_by_branchId_and_shopNo(sTradeInfo.upBranchId, "9999", &sUpBranchInfo);
						if(res != TTS_SUCCESS)
						{
							DB_TradeInfo_free_lock(&lockInfo);
							dbRollback();
							ELOG(ERROR, "[����:%s_%s]Update���ݿ��[TRADEINFO][branchId:%s][shopNo:9999],ERR:%d", 
								session->servCode, session->sessionId, sTradeInfo.upBranchId, res);
							SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, NULL, NULL);
							return SERVICE_EXCEPTION(SYSTEM);
						}
						dbCommit();
					}
				}
__END__:
#if     1       //add by jdw@20160504 for ����customerIdѡ���̻��� begin
                res = GetMerchantIdByCustomer(session, jnls, merType, sUpBranchInfo.branchId);
                if (res)
                {
                    DB_TradeInfo_free_lock(&lockInfo);
                    ELOG(ERROR, "[����:%s] ����CUSTOMER��ȡ�̻���ʧ��,ERR:[DB:%d]", session->servCode, res);
                    SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, NULL, NULL);
                    return SERVICE_EXCEPTION(SYSTEM);
                }
#endif          //add by jdw@20160504 for ����customerIdѡ���̻��� end

				DataPoolPutString(session->dataPool, TX_MP_ECARD_PWD_FLAG, merType); 
				DataPoolPutString(session->dataPool, TX_HOST_MERCHANT_ID, jnls->hostMerchId);
				DataPoolPutString(session->dataPool, TX_HOST_TERMINAL, jnls->hostTermId);
				DB_TradeInfo_free_lock(&lockInfo);
			}
   		}
	

   /* ��ȡǩ����ַ */  
#if 0
   if(strcmp("EXC003",jnls->tradeCode) == 0)
   {   
		   PREP_SIGN_UPLOAD    sPrep_Sign_Upload;
		   char corder[21] = {0};

		   dataLen = 32; 
		   DataPoolGetString2(session->dataPool, TX_ORDER_NO, corder, &dataLen);
		   res = DB_PREP_SIGN_UPLOAD_read_by_orderId(corder, &sPrep_Sign_Upload);
		   if (res)
		   {   
				   ELOG(ERROR, "[����:%s] ���ݶ����Ż�ȡ���ݿ��[PREP_SIGN_UPLOAD]ʧ��,ERR:[DB:%d]", session->servCode, res);
				   SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_D, NULL, NULL);
				   return SERVICE_EXCEPTION(SYSTEM);
		   }   

		   DataPoolPutString(session->dataPool, 152, sPrep_Sign_Upload.signPicasciiStrurl);
   }   
#endif

MODULES_END:
   /*�û�GPS��Ϣ*/
   	dataLen = 20;
   	DataPoolGetString2(session->dataPool, TX_LATITUDE, jnls->latitude, &dataLen);

   	dataLen = 20;
   	DataPoolGetString2(session->dataPool, TX_LONGITUDE, jnls->longitude, &dataLen);

   	dataLen = 20;
   	DataPoolGetString2(session->dataPool, TX_IP2, jnls->txnip, &dataLen);

    /* add by jdw@20160504 for ���� ʡ�� begin*/
   	dataLen = 50;
   	DataPoolGetString2(session->dataPool, TX_PROVINCE_NAME, jnls->province, &dataLen);
    delspace(jnls->province);

   	dataLen = 50;
   	DataPoolGetString2(session->dataPool, TX_CITY_NAME, jnls->city, &dataLen);
    delspace(jnls->city);
    ELOG(INFO, "PROVINCE[%s], CITY[%s]", jnls->province, jnls->city);
    /* add by jdw@20160504 for ���� ʡ�� end*/

   	res = DB_PayJnls_add(jnls);
   	if ( res )
   	{
   		dbRollback();
   		ELOG(ERROR, "[����:%s_%s]Add���ݿ�[PayJnls][termId:%s]ʧ��,ERR:%d", session->servCode, session->sessionId, 
			jnls->termId, res);
   		SET_EXCEPTION(session, LOCAL, SYSTEM , RUN_TO_A, NULL, NULL);
   		return SERVICE_EXCEPTION(SYSTEM);
   	}

   	dbCommit();
	
   	ELOG(INFO, "�˳�ģ�� mod_mbpay_register_pay_jnls");

   	return TTS_SUCCESS;
}

DEFINE_ENGINE_MODULE(mod_mbpay_register_pay_jnls, 52, 1000, 1, "mbpay_register_pay_jnls", invoke_mbpay_register_pay_jnls);


/** @} */

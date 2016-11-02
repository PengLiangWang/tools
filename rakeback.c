/**< */

#include "rakeback.h"
#include "tpospub.h"

int done = 0;

int TCallInit(FBFR32 **sndBuf, FBFR32 **rcvBuf)
{
   *sndBuf = (FBFR32 *)tpalloc(FMLTYPE32, NULL, 4196);
   if(*sndBuf == NULL)
   {
      ELOG(ERROR,"���ú��� tpalloc ʧ��(%s:%d)", tpstrerror(tperrno), tperrno);
      return TTS_ENOMEM;
   }
   *rcvBuf = (FBFR32 *)tpalloc(FMLTYPE32, NULL, 4196);
   if(*rcvBuf == NULL)
   {
      ELOG(ERROR,"���ú��� tpalloc ʧ��(%s:%d)", tpstrerror(tperrno), tperrno);
      tpfree((char *)*sndBuf);
      *sndBuf = NULL;
      return TTS_ENOMEM;
   }

   return TTS_SUCCESS;
}

int TCallTerm(FBFR32 **sndBuf, FBFR32 **rcvBuf)
{
   if(*sndBuf)
   {
      tpfree((char *)*sndBuf);
      *sndBuf = NULL;
   }
   if(*rcvBuf)
   {
      tpfree((char *)*rcvBuf);
      *rcvBuf = NULL;
   }
   tpterm();
   return TTS_SUCCESS;
}

int TCallFml32(char *service, FBFR32 *sndBuf, FBFR32 **rcvBuf)
{
   int  res, len;
   ELOG(INFO, "WSNADDR:[%s]", (char *)tuxgetenv("WSNADDR")); 
   res = tpcall(service, (char *)sndBuf, 0L ,(char **)rcvBuf, (long *)&len, 0);
   if(res != TTS_SUCCESS)
   {
       ELOG(ERROR,"tpcall����[%s]ʧ��res:(%s:%d) ", service, tpstrerror(tperrno), res);
       return res;
   }
   ELOG(INFO, "tpcall����[%s]�ɹ�", service);
   return TTS_SUCCESS;
}

int PA0010ToPayAcctFml32(FBFR32 *sndBuf, char *logNo ,char *customerId, int rfbamount)
{
	int                     res;
	FLDOCC32                fml32Oc = -1;
    TDateTime               tradeDT;

	ELOG(INFO, "���뺯�� PA0010ToPayAcctFml32");

	Finit32(sndBuf, Fsizeof32(sndBuf));
    GetCurrentDateTime(&tradeDT);

	res = Fchg32(sndBuf, SYS_FLD_TRADE_CODE, fml32Oc, "PA0010", 6);
	if(res < 0)
	{   
		ELOG(ERROR, "дFml32(%d)ʧ��(Ferror32:%d)", SYS_FLD_TRADE_CODE, Ferror32);
		res = TTS_EGENERAL;
		return res;
	}

	res = Fchg32(sndBuf, TRN_TX_TIME, fml32Oc, tradeDT.hms1, 6);//���ؽ���ʱ��
	if(res < 0)
	{
		ELOG(ERROR, "дFml32(%d)ʧ��(Ferror32:%d)", TRN_TX_TIME, Ferror32);
		res = TTS_EGENERAL;
		return res;
	}

	res = Fchg32(sndBuf, TRN_TX_DATE, fml32Oc, tradeDT.ymd1, 8);//���ؽ�������
	if(res < 0)
	{
		ELOG(ERROR, "дFml32(%d)ʧ��(Ferror32:%d)", TRN_TX_DATE, Ferror32);
		res = TTS_EGENERAL;
		return res;
	}

	res = Fchg32(sndBuf, TRADE_KEY, fml32Oc, logNo, 6);//tradekey
	if(res < 0)
	{
		ELOG(ERROR, "дFml32(%d)ʧ��(Ferror32:%d)", TRADE_KEY, Ferror32);
		res = TTS_EGENERAL;
		return res;
	}

	res = Fchg32(sndBuf, ACC_TXNACQINSTID, fml32Oc, "00800075", 8);
	if(res < 0)
	{
		ELOG(ERROR, "дFml32(%d)ʧ��(Ferror32:%d)", ACC_TXNACQINSTID, Ferror32);
		res = TTS_EGENERAL;
		return res;
	}

	res = Fchg32(sndBuf, ACC_TXNSTTLDATE, fml32Oc, tradeDT.ymd1, 8);
	if(res < 0)
	{
		ELOG(ERROR, "дFml32(%d)ʧ��(Ferror32:%d)", ACC_TXNSTTLDATE, Ferror32);
		res = TTS_EGENERAL;
		return res;
	}

	res = Fchg32(sndBuf, ACC_CUSTOMERID, fml32Oc, "00800075", 8);
	if(res < 0)
	{
		ELOG(ERROR, "дFml32(%d)ʧ��(Ferror32:%d)", ACC_CUSTOMERID, Ferror32);
		res = TTS_EGENERAL;
		return res;
	}

	res = Fchg32(sndBuf, ACC_ACCTTYPE, fml32Oc, "00", 2);
	if(res < 0)
	{
		ELOG(ERROR, "дFml32(%d)ʧ��(Ferror32:%d)", ACC_ACCTTYPE, Ferror32);
		res = TTS_EGENERAL;
		return res;
	}

    char amount[13] = {0};
    sprintf(amount, "%012d", rfbamount);
	res = Fchg32(sndBuf, ACC_TXNAMT, fml32Oc, amount, 12);
	if(res < 0)
	{
		ELOG(ERROR, "дFml32(%d)ʧ��(Ferror32:%d)", ACC_TXNAMT, Ferror32);
		res = TTS_EGENERAL;
		return res;
	}

	res = Fchg32(sndBuf, ACC_TXNFUNDSINSTID, fml32Oc, "00800001", 8);
	if(res < 0)
	{
		ELOG(ERROR, "дFml32(%d)ʧ��(Ferror32:%d)", ACC_TXNFUNDSINSTID, Ferror32);
		res = TTS_EGENERAL;
		return res;
	}

	res = Fchg32(sndBuf, ACC_TXNFUNDS, fml32Oc, "01", 2);
	if(res < 0)
	{
		ELOG(ERROR, "дFml32(%d)ʧ��(Ferror32:%d)", ACC_TXNFUNDS, Ferror32);
		res = TTS_EGENERAL;
		return res;
	}

	res = Fchg32(sndBuf, ACC_TXSHOPNO, fml32Oc, "NULL", 4);
	if(res < 0)
	{
		ELOG(ERROR, "дFml32(%d)ʧ��(Ferror32:%d)", ACC_TXSHOPNO, Ferror32);
		res = TTS_EGENERAL;
		return res;
	}

	res = Fchg32(sndBuf, ACC_CUSTOMERID2, fml32Oc, customerId, 12);
	if(res < 0)
	{
		ELOG(ERROR, "дFml32(%d)ʧ��(Ferror32:%d)", ACC_CUSTOMERID2, Ferror32);
		res = TTS_EGENERAL;
		return res;
	}

	res = Fchg32(sndBuf, ACC_ACCTTYPE2, fml32Oc, "12", 2);
	if(res < 0)
	{
		ELOG(ERROR, "дFml32(%d)ʧ��(Ferror32:%d)", ACC_ACCTTYPE2, Ferror32);
		res = TTS_EGENERAL;
		return res;
	}

	res = Fchg32(sndBuf, ACC_DOCTYPE, fml32Oc, "01", 2);
	if(res < 0)
	{
		ELOG(ERROR, "дFml32(%d)ʧ��(Ferror32:%d)", ACC_DOCTYPE, Ferror32);
		res = TTS_EGENERAL;
		return res;
	}

	res = Fchg32(sndBuf, ACC_DOCNO, fml32Oc, logNo, 6);
	if(res < 0)
	{
		ELOG(ERROR, "дFml32(%d)ʧ��(Ferror32:%d)", ACC_DOCNO, Ferror32);
		res = TTS_EGENERAL;
		return res;
	}

	res = Fchg32(sndBuf, ACC_CHECKERID, fml32Oc, "B001", 4);
	if(res < 0)
	{
		ELOG(ERROR, "дFml32(%d)ʧ��(Ferror32:%d)", ACC_CHECKERID, Ferror32);
		res = TTS_EGENERAL;
		return res;
	}

	res = Fchg32(sndBuf, ACC_TELLERID, fml32Oc, "A001", 4);
	if(res < 0)
	{
		ELOG(ERROR, "дFml32(%d)ʧ��(Ferror32:%d)", ACC_TELLERID, Ferror32);
		res = TTS_EGENERAL;
		return res;
	}

	ELOG(INFO, "�˳����� PA0010ToPayAcctFml32");

	return TTS_SUCCESS;
}

void thread_task(void *arg)
{
    FBFR32                  *sndBuf = NULL,
					        *rcvBuf = NULL;
    int                     res;
    FLDOCC32                fml32Oc;
    FLDLEN32                fml32Len = 0;
    char                    logNo[7] = {0};
    char                    msgCode[3] = {0};

    ELOG(INFO, "���� thread_task ģ��.");

    usleep(100000);   
    ELOG(INFO, "usleep 100000");
    pthread_mutex_lock(&lock);
    TB_Fans_Customer_Jnls  *tfFansCusJnls;
    tfFansCusJnls = (TB_Fans_Customer_Jnls *)arg;
    ELOG(INFO, "********��ʼ�����û�<%s>�ķ�Ӷ, ������ˮ��<%s>***********",tfFansCusJnls->customerId ,tfFansCusJnls->localLogNo);
    strncpy(tfFansCusJnls->is_clearing, "333", 3);   //���˴�����
    res = DB_TB_Fans_Customer_Jnls_update_by_localDate_and_localTime_and_localLogNo(tfFansCusJnls->localDate, tfFansCusJnls->localTime, tfFansCusJnls->localLogNo, tfFansCusJnls);
    if(res != TTS_SUCCESS)
    {
        dbRollback();
        ELOG(ERROR, "Update �� TB_Fans_Customer_Jnls ʧ��, ERR: %s", res);    
        return;
    }
    dbCommit();
    pthread_mutex_unlock(&lock);
    if(tfFansCusJnls->parent_profit == 0)
        tfFansCusJnls->is_clearing[0]='1';
    if(tfFansCusJnls->two_profit == 0)
        tfFansCusJnls->is_clearing[1]='1';
    if(tfFansCusJnls->top_profit == 0)
        tfFansCusJnls->is_clearing[2]='1';

    if(tfFansCusJnls->parent_profit != 0)
    {
        res = TCallInit(&sndBuf, &rcvBuf);
        if(res != TTS_SUCCESS)
        {
            ELOG(ERROR,"��ʼ�� tuxedo ʧ��. ERR: %d", res);
            tfFansCusJnls->is_clearing[0]='2';
            goto _UPDATE_;
        }
        res = PA0010ToPayAcctFml32(sndBuf, tfFansCusJnls->localLogNo, tfFansCusJnls->parent_customerId, tfFansCusJnls->parent_profit);
        if(res)
        {
            ELOG(ERROR, "�ϼ���Ӷ���ʧ��");
            tfFansCusJnls->is_clearing[0]='2';
            goto _UPDATE_;
        }
        res = TCallFml32("PersonalTrade", sndBuf, &rcvBuf);
        if(res != TTS_SUCCESS)
        {
            ELOG(ERROR, "������ͨѶʧ��");
            tfFansCusJnls->is_clearing[0]='2';
            goto _UPDATE_;
        }
        fml32Oc  = 0;
        fml32Len = sizeof(msgCode);
        res = Fget32(rcvBuf, MSG_CODE, fml32Oc,msgCode, &fml32Len);
        if(res < 0)
        {   
            ELOG(ERROR, "��ȡFML����(%d)ʧ��,FMLʧ����Ϣ(Ferror32:%d)", MSG_CODE, Ferror32);       //״̬����, �����������е�״̬
            return;
        }   
        ELOG(INFO,"һ����Ӷ<customerId: %s>�ɹ�msgCode<%s>��� %d ��",tfFansCusJnls->parent_customerId,  msgCode, tfFansCusJnls->parent_profit);
        if(strlen(msgCode) != 0 && strncmp(msgCode, "00", 2) == 0)     //ֻ��msgCode����Ϊ0���������˳ɹ�,�Ÿ���Ϊ1������ȫΪʧ�ܸ���Ϊ2
            tfFansCusJnls->is_clearing[0] = '1';
    }
    if(tfFansCusJnls->two_profit != 0)
    {
         res = TCallInit(&sndBuf, &rcvBuf);
         if(res != TTS_SUCCESS)
         {
             ELOG(ERROR,"��ʼ�� tuxedo ʧ��. ERR: %d", res);
             tfFansCusJnls->is_clearing[1]='2';
             goto _UPDATE_;
         }
         res = GetSerialNo(11, "pay", logNo, 6);
         if(res != TTS_SUCCESS)
         {
             ELOG(ERROR, "��ȡ������ˮʧ��: %d", res);
             tfFansCusJnls->is_clearing[1]='2';
             goto _UPDATE_;
         }
         res = PA0010ToPayAcctFml32(sndBuf, logNo, tfFansCusJnls->two_customerId, tfFansCusJnls->two_profit);
         if(res)
         {
             ELOG(ERROR, "������Ӷ���ʧ��");
             tfFansCusJnls->is_clearing[1]='2';
             goto _UPDATE_;
         }
        res = TCallFml32("PersonalTrade", sndBuf, &rcvBuf);
        if(res != TTS_SUCCESS)
        {
            ELOG(ERROR, "������ͨѶʧ��");
            tfFansCusJnls->is_clearing[1]='2';
            goto _UPDATE_;
        }
        fml32Oc  = 0;
        fml32Len = sizeof(msgCode);
        res = Fget32(rcvBuf, MSG_CODE, fml32Oc,msgCode, &fml32Len);
        if(res < 0)
        {   
            ELOG(ERROR, "��ȡFML����(%d)ʧ��,FMLʧ����Ϣ(Ferror32:%d)", MSG_CODE, Ferror32);
            return;
        }   
        ELOG(INFO, "������Ӷ<customerId: %s>�ɹ�msgCode<%s>��� %d ��", tfFansCusJnls->two_customerId, msgCode, tfFansCusJnls->two_profit);
        if(strlen(msgCode) != 0 && strncmp(msgCode, "00", 2) == 0)     //ֻ��msgCode����Ϊ00, �������˳ɹ�,�Ÿ���Ϊ1������ȫΪʧ�ܸ���Ϊ2
            tfFansCusJnls->is_clearing[1] = '1';
    }
    if(tfFansCusJnls->top_profit != 0)
    {
        res = TCallInit(&sndBuf, &rcvBuf);
        if(res != TTS_SUCCESS)
        {
            ELOG(ERROR,"��ʼ�� tuxedo ʧ��. ERR: %d", res);
            tfFansCusJnls->is_clearing[2]='2';
            goto _UPDATE_;
        }
        res = GetSerialNo(11, "pay", logNo, 6);
        if(res != TTS_SUCCESS)
        {
            ELOG(ERROR, "��ȡ������ˮʧ��: %d", res);
            tfFansCusJnls->is_clearing[2]='2';
            goto _UPDATE_;
        }
        res = PA0010ToPayAcctFml32(sndBuf, logNo, tfFansCusJnls->top_customerId, tfFansCusJnls->top_profit);
        if(res)
        {
            ELOG(ERROR, "������Ӷ���ʧ��");
            tfFansCusJnls->is_clearing[2]='2';
            goto _UPDATE_;
        }
        res = TCallFml32("PersonalTrade", sndBuf, &rcvBuf);
        if(res != TTS_SUCCESS)
        {
            ELOG(ERROR, "������ͨѶʧ��");
            tfFansCusJnls->is_clearing[2]='2';
            goto _UPDATE_;
        }
        fml32Oc  = 0;
        fml32Len = sizeof(msgCode);
        res = Fget32(rcvBuf, MSG_CODE, fml32Oc,msgCode, &fml32Len);
        if(res < 0)
        {   
            ELOG(ERROR, "��ȡFML����(%d)ʧ��,FMLʧ����Ϣ(Ferror32:%d)", MSG_CODE, Ferror32);
            return;
        }   
        ELOG(INFO, "������Ӷ<customerId: %s>�ɹ�msgCode<%s>��� %d ��", tfFansCusJnls->top_customerId, msgCode, tfFansCusJnls->top_profit);
        if(strlen(msgCode) != 0 && strncmp(msgCode, "00", 2) == 0)     //ֻ��msgCode����Ϊ00, �������˳ɹ�,�Ÿ���Ϊ1������ȫΪʧ�ܸ���Ϊ2
            tfFansCusJnls->is_clearing[2] = '1';
    }
_UPDATE_:
    res = DB_TB_Fans_Customer_Jnls_update_by_localDate_and_localTime_and_localLogNo(tfFansCusJnls->localDate, tfFansCusJnls->localTime, tfFansCusJnls->localLogNo, tfFansCusJnls);
    if(res != TTS_SUCCESS)
    {
        dbRollback();
        ELOG(ERROR, "Update �� TB_Fans_Customer_Jnls ʧ��, ERR: %s", res);    
        return;
    }
    dbCommit();
    ELOG(INFO, "*********�û�<%s>�Ľ��׷�Ӷ�Ѵ������,������ˮ��<%s>***********", tfFansCusJnls->customerId, tfFansCusJnls->localLogNo);
    done++;
    return;
}

//int rakebackaction(void *pvBuff)
int rakebackaction()
{
    int                      res = 0, tasks=0;
    char                    JsonMsg[100] = {0};
    char                    status[2] = {0};
    TB_Fans_Customer_Jnls   TFCustomerJnls;
    Select_Info             sInfo;
    threadpool_t            *pool;   //�̳߳�
    TB_Fans_Customer_Jnls   tFcustomer[1000];

    ELOG(INFO, "���� rakebackaction ģ��.");

//    memcpy(JsonMsg, (char *)pvBuff, strlen((char *)pvBuff));
//    ELOG(INFO, "���ձ���: %s", JsonMsg);

    pthread_mutex_init(&lock, NULL);    //��ʼ��������
    //�����̳߳�
    assert((pool = threadpool_create(THREAD, QUEUE, 0)) != NULL);
    ELOG(INFO, "Pool started with %d threads and queue size of %d\n", THREAD, QUEUE);
    /*
    res = GetStrFromJson1(JsonMsg, "STATUS", status);
    if(res)
    {
        ELOG(ERROR,"Json ����ʧ��<STATUS>, ERR: %d", res);
        return -1;
    }
    if(strncmp(status, "1", 1) == 0)    //ƽ̨�������ͨ��
    */
    {
        memset(&sInfo, 0x00, sizeof(sInfo));
        res = DB_TB_Fans_Customer_Jnls_open_select_by_status_and_is_clearing("00", "000", &sInfo);
        if(res != TTS_SUCCESS)
        {
            ELOG(ERROR, "�򿪿ͻ�������ˮ�� TB_Fans_Customer_Jnls ʧ��, ERR: %d", res);
            return res;
        }
        while(1)
        {
            memset(&TFCustomerJnls, 0x00, sizeof(TB_Fans_Customer_Jnls));
            res = DB_TB_Fans_Customer_Jnls_fetch_select(&sInfo, &TFCustomerJnls);
            if(res != TTS_SUCCESS && res != SQLNOTFOUND)
            {
                ELOG(ERROR, "Fetch �� TB_Fans_Customer_Jnls ʧ��, ERR: %d", res);
                DB_TB_Fans_Customer_Jnls_close_select(&sInfo);
                return res;
            }
            else if(res == SQLNOTFOUND)
            {
                pthread_mutex_unlock(&lock);
                break;
            }
            ELOG(ERROR, "Fetch ���ݳɹ�.");
            tFcustomer[tasks] = TFCustomerJnls; 
            if(threadpool_add(pool, &thread_task, (void *)&tFcustomer[tasks], 0) == 0)    //������񵽶��� 
            {
                pthread_mutex_lock(&lock);
                ELOG(INFO, "�������: %d, ��ˮ��: %s", tasks++, TFCustomerJnls.localLogNo);
                pthread_mutex_unlock(&lock);
            }
        }
    }
    DB_TB_Fans_Customer_Jnls_close_select(&sInfo);
    assert(threadpool_destroy(pool, 1) == 0);     //ִ�����̳߳��е��̲߳������̳߳�
    ELOG(INFO, "������������: %d", done);
    ELOG(INFO, "��Ӷ���˴������!");
    return TTS_SUCCESS;
}

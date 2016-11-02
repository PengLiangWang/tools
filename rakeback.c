/**< */

#include "rakeback.h"
#include "tpospub.h"

int done = 0;

int TCallInit(FBFR32 **sndBuf, FBFR32 **rcvBuf)
{
   *sndBuf = (FBFR32 *)tpalloc(FMLTYPE32, NULL, 4196);
   if(*sndBuf == NULL)
   {
      ELOG(ERROR,"调用函数 tpalloc 失败(%s:%d)", tpstrerror(tperrno), tperrno);
      return TTS_ENOMEM;
   }
   *rcvBuf = (FBFR32 *)tpalloc(FMLTYPE32, NULL, 4196);
   if(*rcvBuf == NULL)
   {
      ELOG(ERROR,"调用函数 tpalloc 失败(%s:%d)", tpstrerror(tperrno), tperrno);
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
       ELOG(ERROR,"tpcall服务[%s]失败res:(%s:%d) ", service, tpstrerror(tperrno), res);
       return res;
   }
   ELOG(INFO, "tpcall服务[%s]成功", service);
   return TTS_SUCCESS;
}

int PA0010ToPayAcctFml32(FBFR32 *sndBuf, char *logNo ,char *customerId, int rfbamount)
{
	int                     res;
	FLDOCC32                fml32Oc = -1;
    TDateTime               tradeDT;

	ELOG(INFO, "进入函数 PA0010ToPayAcctFml32");

	Finit32(sndBuf, Fsizeof32(sndBuf));
    GetCurrentDateTime(&tradeDT);

	res = Fchg32(sndBuf, SYS_FLD_TRADE_CODE, fml32Oc, "PA0010", 6);
	if(res < 0)
	{   
		ELOG(ERROR, "写Fml32(%d)失败(Ferror32:%d)", SYS_FLD_TRADE_CODE, Ferror32);
		res = TTS_EGENERAL;
		return res;
	}

	res = Fchg32(sndBuf, TRN_TX_TIME, fml32Oc, tradeDT.hms1, 6);//本地交易时间
	if(res < 0)
	{
		ELOG(ERROR, "写Fml32(%d)失败(Ferror32:%d)", TRN_TX_TIME, Ferror32);
		res = TTS_EGENERAL;
		return res;
	}

	res = Fchg32(sndBuf, TRN_TX_DATE, fml32Oc, tradeDT.ymd1, 8);//本地交易日期
	if(res < 0)
	{
		ELOG(ERROR, "写Fml32(%d)失败(Ferror32:%d)", TRN_TX_DATE, Ferror32);
		res = TTS_EGENERAL;
		return res;
	}

	res = Fchg32(sndBuf, TRADE_KEY, fml32Oc, logNo, 6);//tradekey
	if(res < 0)
	{
		ELOG(ERROR, "写Fml32(%d)失败(Ferror32:%d)", TRADE_KEY, Ferror32);
		res = TTS_EGENERAL;
		return res;
	}

	res = Fchg32(sndBuf, ACC_TXNACQINSTID, fml32Oc, "00800075", 8);
	if(res < 0)
	{
		ELOG(ERROR, "写Fml32(%d)失败(Ferror32:%d)", ACC_TXNACQINSTID, Ferror32);
		res = TTS_EGENERAL;
		return res;
	}

	res = Fchg32(sndBuf, ACC_TXNSTTLDATE, fml32Oc, tradeDT.ymd1, 8);
	if(res < 0)
	{
		ELOG(ERROR, "写Fml32(%d)失败(Ferror32:%d)", ACC_TXNSTTLDATE, Ferror32);
		res = TTS_EGENERAL;
		return res;
	}

	res = Fchg32(sndBuf, ACC_CUSTOMERID, fml32Oc, "00800075", 8);
	if(res < 0)
	{
		ELOG(ERROR, "写Fml32(%d)失败(Ferror32:%d)", ACC_CUSTOMERID, Ferror32);
		res = TTS_EGENERAL;
		return res;
	}

	res = Fchg32(sndBuf, ACC_ACCTTYPE, fml32Oc, "00", 2);
	if(res < 0)
	{
		ELOG(ERROR, "写Fml32(%d)失败(Ferror32:%d)", ACC_ACCTTYPE, Ferror32);
		res = TTS_EGENERAL;
		return res;
	}

    char amount[13] = {0};
    sprintf(amount, "%012d", rfbamount);
	res = Fchg32(sndBuf, ACC_TXNAMT, fml32Oc, amount, 12);
	if(res < 0)
	{
		ELOG(ERROR, "写Fml32(%d)失败(Ferror32:%d)", ACC_TXNAMT, Ferror32);
		res = TTS_EGENERAL;
		return res;
	}

	res = Fchg32(sndBuf, ACC_TXNFUNDSINSTID, fml32Oc, "00800001", 8);
	if(res < 0)
	{
		ELOG(ERROR, "写Fml32(%d)失败(Ferror32:%d)", ACC_TXNFUNDSINSTID, Ferror32);
		res = TTS_EGENERAL;
		return res;
	}

	res = Fchg32(sndBuf, ACC_TXNFUNDS, fml32Oc, "01", 2);
	if(res < 0)
	{
		ELOG(ERROR, "写Fml32(%d)失败(Ferror32:%d)", ACC_TXNFUNDS, Ferror32);
		res = TTS_EGENERAL;
		return res;
	}

	res = Fchg32(sndBuf, ACC_TXSHOPNO, fml32Oc, "NULL", 4);
	if(res < 0)
	{
		ELOG(ERROR, "写Fml32(%d)失败(Ferror32:%d)", ACC_TXSHOPNO, Ferror32);
		res = TTS_EGENERAL;
		return res;
	}

	res = Fchg32(sndBuf, ACC_CUSTOMERID2, fml32Oc, customerId, 12);
	if(res < 0)
	{
		ELOG(ERROR, "写Fml32(%d)失败(Ferror32:%d)", ACC_CUSTOMERID2, Ferror32);
		res = TTS_EGENERAL;
		return res;
	}

	res = Fchg32(sndBuf, ACC_ACCTTYPE2, fml32Oc, "12", 2);
	if(res < 0)
	{
		ELOG(ERROR, "写Fml32(%d)失败(Ferror32:%d)", ACC_ACCTTYPE2, Ferror32);
		res = TTS_EGENERAL;
		return res;
	}

	res = Fchg32(sndBuf, ACC_DOCTYPE, fml32Oc, "01", 2);
	if(res < 0)
	{
		ELOG(ERROR, "写Fml32(%d)失败(Ferror32:%d)", ACC_DOCTYPE, Ferror32);
		res = TTS_EGENERAL;
		return res;
	}

	res = Fchg32(sndBuf, ACC_DOCNO, fml32Oc, logNo, 6);
	if(res < 0)
	{
		ELOG(ERROR, "写Fml32(%d)失败(Ferror32:%d)", ACC_DOCNO, Ferror32);
		res = TTS_EGENERAL;
		return res;
	}

	res = Fchg32(sndBuf, ACC_CHECKERID, fml32Oc, "B001", 4);
	if(res < 0)
	{
		ELOG(ERROR, "写Fml32(%d)失败(Ferror32:%d)", ACC_CHECKERID, Ferror32);
		res = TTS_EGENERAL;
		return res;
	}

	res = Fchg32(sndBuf, ACC_TELLERID, fml32Oc, "A001", 4);
	if(res < 0)
	{
		ELOG(ERROR, "写Fml32(%d)失败(Ferror32:%d)", ACC_TELLERID, Ferror32);
		res = TTS_EGENERAL;
		return res;
	}

	ELOG(INFO, "退出函数 PA0010ToPayAcctFml32");

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

    ELOG(INFO, "进入 thread_task 模块.");

    usleep(100000);   
    ELOG(INFO, "usleep 100000");
    pthread_mutex_lock(&lock);
    TB_Fans_Customer_Jnls  *tfFansCusJnls;
    tfFansCusJnls = (TB_Fans_Customer_Jnls *)arg;
    ELOG(INFO, "********开始处理用户<%s>的返佣, 处理流水号<%s>***********",tfFansCusJnls->customerId ,tfFansCusJnls->localLogNo);
    strncpy(tfFansCusJnls->is_clearing, "333", 3);   //入账处理中
    res = DB_TB_Fans_Customer_Jnls_update_by_localDate_and_localTime_and_localLogNo(tfFansCusJnls->localDate, tfFansCusJnls->localTime, tfFansCusJnls->localLogNo, tfFansCusJnls);
    if(res != TTS_SUCCESS)
    {
        dbRollback();
        ELOG(ERROR, "Update 表 TB_Fans_Customer_Jnls 失败, ERR: %s", res);    
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
            ELOG(ERROR,"初始化 tuxedo 失败. ERR: %d", res);
            tfFansCusJnls->is_clearing[0]='2';
            goto _UPDATE_;
        }
        res = PA0010ToPayAcctFml32(sndBuf, tfFansCusJnls->localLogNo, tfFansCusJnls->parent_customerId, tfFansCusJnls->parent_profit);
        if(res)
        {
            ELOG(ERROR, "上级返佣组包失败");
            tfFansCusJnls->is_clearing[0]='2';
            goto _UPDATE_;
        }
        res = TCallFml32("PersonalTrade", sndBuf, &rcvBuf);
        if(res != TTS_SUCCESS)
        {
            ELOG(ERROR, "与主机通讯失败");
            tfFansCusJnls->is_clearing[0]='2';
            goto _UPDATE_;
        }
        fml32Oc  = 0;
        fml32Len = sizeof(msgCode);
        res = Fget32(rcvBuf, MSG_CODE, fml32Oc,msgCode, &fml32Len);
        if(res < 0)
        {   
            ELOG(ERROR, "获取FML数据(%d)失败,FML失败信息(Ferror32:%d)", MSG_CODE, Ferror32);       //状态不明, 保持在入账中的状态
            return;
        }   
        ELOG(INFO,"一级返佣<customerId: %s>成功msgCode<%s>金额 %d 分",tfFansCusJnls->parent_customerId,  msgCode, tfFansCusJnls->parent_profit);
        if(strlen(msgCode) != 0 && strncmp(msgCode, "00", 2) == 0)     //只有msgCode返回为0，才是入账成功,才更新为1，其余全为失败更新为2
            tfFansCusJnls->is_clearing[0] = '1';
    }
    if(tfFansCusJnls->two_profit != 0)
    {
         res = TCallInit(&sndBuf, &rcvBuf);
         if(res != TTS_SUCCESS)
         {
             ELOG(ERROR,"初始化 tuxedo 失败. ERR: %d", res);
             tfFansCusJnls->is_clearing[1]='2';
             goto _UPDATE_;
         }
         res = GetSerialNo(11, "pay", logNo, 6);
         if(res != TTS_SUCCESS)
         {
             ELOG(ERROR, "获取交易流水失败: %d", res);
             tfFansCusJnls->is_clearing[1]='2';
             goto _UPDATE_;
         }
         res = PA0010ToPayAcctFml32(sndBuf, logNo, tfFansCusJnls->two_customerId, tfFansCusJnls->two_profit);
         if(res)
         {
             ELOG(ERROR, "二级返佣组包失败");
             tfFansCusJnls->is_clearing[1]='2';
             goto _UPDATE_;
         }
        res = TCallFml32("PersonalTrade", sndBuf, &rcvBuf);
        if(res != TTS_SUCCESS)
        {
            ELOG(ERROR, "与主机通讯失败");
            tfFansCusJnls->is_clearing[1]='2';
            goto _UPDATE_;
        }
        fml32Oc  = 0;
        fml32Len = sizeof(msgCode);
        res = Fget32(rcvBuf, MSG_CODE, fml32Oc,msgCode, &fml32Len);
        if(res < 0)
        {   
            ELOG(ERROR, "获取FML数据(%d)失败,FML失败信息(Ferror32:%d)", MSG_CODE, Ferror32);
            return;
        }   
        ELOG(INFO, "二级返佣<customerId: %s>成功msgCode<%s>金额 %d 分", tfFansCusJnls->two_customerId, msgCode, tfFansCusJnls->two_profit);
        if(strlen(msgCode) != 0 && strncmp(msgCode, "00", 2) == 0)     //只有msgCode返回为00, 才是入账成功,才更新为1，其余全为失败更新为2
            tfFansCusJnls->is_clearing[1] = '1';
    }
    if(tfFansCusJnls->top_profit != 0)
    {
        res = TCallInit(&sndBuf, &rcvBuf);
        if(res != TTS_SUCCESS)
        {
            ELOG(ERROR,"初始化 tuxedo 失败. ERR: %d", res);
            tfFansCusJnls->is_clearing[2]='2';
            goto _UPDATE_;
        }
        res = GetSerialNo(11, "pay", logNo, 6);
        if(res != TTS_SUCCESS)
        {
            ELOG(ERROR, "获取交易流水失败: %d", res);
            tfFansCusJnls->is_clearing[2]='2';
            goto _UPDATE_;
        }
        res = PA0010ToPayAcctFml32(sndBuf, logNo, tfFansCusJnls->top_customerId, tfFansCusJnls->top_profit);
        if(res)
        {
            ELOG(ERROR, "三级返佣组包失败");
            tfFansCusJnls->is_clearing[2]='2';
            goto _UPDATE_;
        }
        res = TCallFml32("PersonalTrade", sndBuf, &rcvBuf);
        if(res != TTS_SUCCESS)
        {
            ELOG(ERROR, "与主机通讯失败");
            tfFansCusJnls->is_clearing[2]='2';
            goto _UPDATE_;
        }
        fml32Oc  = 0;
        fml32Len = sizeof(msgCode);
        res = Fget32(rcvBuf, MSG_CODE, fml32Oc,msgCode, &fml32Len);
        if(res < 0)
        {   
            ELOG(ERROR, "获取FML数据(%d)失败,FML失败信息(Ferror32:%d)", MSG_CODE, Ferror32);
            return;
        }   
        ELOG(INFO, "三级返佣<customerId: %s>成功msgCode<%s>金额 %d 分", tfFansCusJnls->top_customerId, msgCode, tfFansCusJnls->top_profit);
        if(strlen(msgCode) != 0 && strncmp(msgCode, "00", 2) == 0)     //只有msgCode返回为00, 才是入账成功,才更新为1，其余全为失败更新为2
            tfFansCusJnls->is_clearing[2] = '1';
    }
_UPDATE_:
    res = DB_TB_Fans_Customer_Jnls_update_by_localDate_and_localTime_and_localLogNo(tfFansCusJnls->localDate, tfFansCusJnls->localTime, tfFansCusJnls->localLogNo, tfFansCusJnls);
    if(res != TTS_SUCCESS)
    {
        dbRollback();
        ELOG(ERROR, "Update 表 TB_Fans_Customer_Jnls 失败, ERR: %s", res);    
        return;
    }
    dbCommit();
    ELOG(INFO, "*********用户<%s>的交易返佣已处理完毕,处理流水号<%s>***********", tfFansCusJnls->customerId, tfFansCusJnls->localLogNo);
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
    threadpool_t            *pool;   //线程池
    TB_Fans_Customer_Jnls   tFcustomer[1000];

    ELOG(INFO, "进入 rakebackaction 模块.");

//    memcpy(JsonMsg, (char *)pvBuff, strlen((char *)pvBuff));
//    ELOG(INFO, "接收报文: %s", JsonMsg);

    pthread_mutex_init(&lock, NULL);    //初始化互斥锁
    //申请线程池
    assert((pool = threadpool_create(THREAD, QUEUE, 0)) != NULL);
    ELOG(INFO, "Pool started with %d threads and queue size of %d\n", THREAD, QUEUE);
    /*
    res = GetStrFromJson1(JsonMsg, "STATUS", status);
    if(res)
    {
        ELOG(ERROR,"Json 解析失败<STATUS>, ERR: %d", res);
        return -1;
    }
    if(strncmp(status, "1", 1) == 0)    //平台对账审核通过
    */
    {
        memset(&sInfo, 0x00, sizeof(sInfo));
        res = DB_TB_Fans_Customer_Jnls_open_select_by_status_and_is_clearing("00", "000", &sInfo);
        if(res != TTS_SUCCESS)
        {
            ELOG(ERROR, "打开客户分润流水表 TB_Fans_Customer_Jnls 失败, ERR: %d", res);
            return res;
        }
        while(1)
        {
            memset(&TFCustomerJnls, 0x00, sizeof(TB_Fans_Customer_Jnls));
            res = DB_TB_Fans_Customer_Jnls_fetch_select(&sInfo, &TFCustomerJnls);
            if(res != TTS_SUCCESS && res != SQLNOTFOUND)
            {
                ELOG(ERROR, "Fetch 表 TB_Fans_Customer_Jnls 失败, ERR: %d", res);
                DB_TB_Fans_Customer_Jnls_close_select(&sInfo);
                return res;
            }
            else if(res == SQLNOTFOUND)
            {
                pthread_mutex_unlock(&lock);
                break;
            }
            ELOG(ERROR, "Fetch 数据成功.");
            tFcustomer[tasks] = TFCustomerJnls; 
            if(threadpool_add(pool, &thread_task, (void *)&tFcustomer[tasks], 0) == 0)    //添加任务到队列 
            {
                pthread_mutex_lock(&lock);
                ELOG(INFO, "添加任务: %d, 流水号: %s", tasks++, TFCustomerJnls.localLogNo);
                pthread_mutex_unlock(&lock);
            }
        }
    }
    DB_TB_Fans_Customer_Jnls_close_select(&sInfo);
    assert(threadpool_destroy(pool, 1) == 0);     //执行完线程池中的线程才销毁线程池
    ELOG(INFO, "处理任务总数: %d", done);
    ELOG(INFO, "返佣入账处理完毕!");
    return TTS_SUCCESS;
}

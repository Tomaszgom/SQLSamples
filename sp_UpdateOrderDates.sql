/* **********************************************************
* Version: 1.0
* Created By: Tomasz Gomoradzki
* Description: Stored procedure updates Order Dates
* **********************************************************/

create stored procedure spDoSomeJob
@OrderAction VARCHAR(20),  	/* 'Update'/'Review' Order Action Flag if to Select(review) or to Update data*/

@NewChargeDate datetime, 	/* New Charge Date to be updated */
@NewOrderDate datetime,		/* New Order Date to be updated */
@custOrdNum nvarchar(100), 	/* Order Number*/
@ctrl_Num numeric, 			/* Order Control Number */
@PropID numeric,			/* hmy Property ID */

as
begin


declare @bopen numeric;         	/* open OrderTransactionaction */
declare @trParent2ID numeric; 		/* OrderTransactionaction Parent 2 ID */
declare @sys_Ord_Num numeric; 		/* System Order number */
declare @stranOrdNum nvarchar(255); /* TRANOrderNum Order number */
declare @tenantPer numeric; 		/* Tenant ID */
declare @searchBy nvarchar(20); 	/* Search By Flag if to search by Ord_Numb or Ctrl_Num */

declare @CurrentChargeDate datetime; /* Current/Old Charge Due Date to be updated */
declare @CurrentOrderDate datetime; /* Current/Old Order Date to be updated */
declare @FlChargeDateUpdate int; 	/* lag 1 - Update; 0 - No Update */
declare @FlOrderDateUpdate int; 	/* Flag 1 - Update; 0 - No Update */
declare @sys_user varchar(100); 	/* system user and db user */
declare @TabcuOrdTransFlag as Bit; /* Flag: Table 'cuOrdTrans' 1=exist 0=Notexist */

declare @hyCRM numeric; 			/* ID CRM number for DMLHistoryTracker tab tracking changes */
declare @tr_cursor as cursor; 		/* Cursor for DMLHistoryTracker tab tracking changes */
declare @hMy int; 					/* Cursor variable */
declare @EventDate datetime; 	/* Cursor variable */
declare @DATE3 datetime;     	/* Cursor variable */
declare @FixHistNote varchar(500); 	/* Cursor variable */

declare @plcu_cursor as cursor; 	/* Cursor for DMLHistoryTracker tab tracking changes */
declare @plcuhMy int; 				/* Cursor variable */



begin

/* **** Input Validation begin and set @serach by **** */

set @FlChargeDateUpdate = 0
set @FlOrderDateUpdate = 0
set @sys_user = ORIGINAL_LOGIN()+', '+USER+', ID: ' + cast(USER_ID(CURRENT_USER) as nvarchar(100));
set @searchBy = 'custOrdNum'

if @custOrdNum is null OR @custOrdNum = ''
begin
	if @ctrl_Num is null OR @ctrl_Num = 0
		begin
			Select 'Please go back and enter valid ''Order Number'' or ''Control Number''.'
			return
		end
	end
if @NewChargeDate = '' and @NewOrderDate = ''
		begin
			Select 'Please go back and enter valid ''Charge Date'' or ''Order Date''.'
			return
		end
if @NewChargeDate != ''
	set @FlChargeDateUpdate = 1
if @NewOrderDate != ''
	set @FlOrderDateUpdate = 1
/* **** END Input Validation */


/***** START Get Variables */

if @hyCRM = ''
		set @hyCRM = 999
else
	begin
		if len(@hyCRM)>15
			begin
				SELECT 'The Incident number is too long. Max digit is 17. Please go back and try again.'
				return
			end
		else
			begin
			set @hyCRM = CAST(rtrim(ltrim(@hyCRM)) as numeric);
			end
	end


set @custOrdNum = lower(rtrim(ltrim(@custOrdNum)));


/* Only open/Not Paid Orders are accepted. In case this needs to be changed see */
/* below conditionL 'If Order is Paid or Partially Paid then abort' */


   if @Prop_Num = ''
		set @PropID = 0
   else
		set @PropID = CAST(rtrim(ltrim(@Prop_Num)) as numeric);


   if @ctrl_Num = ''
		set @ctrl_Num = 0
   else
		set @ctrl_Num = CAST(rtrim(ltrim(@ctrl_Num)) as numeric);


/***** END Get Variables */


/************************************************************************************************************************/
/************************************************************************************************************************/




/* **** START Search for OrderTransactionaction records (get PARENTID and sys_Ord_Num to identify all OrderTransactionactions to change) */
if @searchBy = 'ctrl_Num'
	begin
		SELECT @sys_Ord_Num = sOrderNum, @stranOrdNum = TRANOrderNum, @trParent2ID = PARENTID, @tenantPer = tenantID 
			FROM OrderTransaction WHERE ITYPE = 7 and CtrlREFNumb = @ctrl_Num and PROPID = @PropID
	end
else if @searchBy = 'custOrdNum'
	begin
		SELECT @sys_Ord_Num = sOrderNum, @stranOrdNum = TRANOrderNum, @trParent2ID = PARENTID, @tenantPer = tenantID
            FROM OrderTransaction WHERE ITYPE = 7 and TRANOrderNum = @custOrdNum and PROPID = @PropID
	end
else
	begin
		Select 'Please go back, enter valid ''Order Number'' or ''Control Number'' and try again.'
		return
	end

if @trParent2ID is null OR @trParent2ID = 0
	begin
		Select 'No matching data or incorrect search values criteria. Please go back and try again.'
		return
	end
if @sys_Ord_Num is null OR @sys_Ord_Num = 0
	begin
		Select 'No matching data or incorrect search values criteria. Please go back and try again.'
		return
	end
if @PropID is null OR @PropID = 0
	begin
		Select 'No matching data or incorrect search values criteria. Please go back and try again.'
		return
	end
	
/***** Create Temp table */
begin
/* Temp Table - OrderTransaction */
if object_id('tempdb.dbo.#tmp_OrderTrans', 'U') is not null
  drop table #tmp_OrderTrans;

  SELECT * into #tmp_OrderTrans FROM OrderTransaction tr
								WHERE 1=1
								and itype = 7
								and PARENTID = @trParent2ID
								and sOrderNum = @sys_Ord_Num
								and PROPID = @PropID
/* Check if Table 'cuOrdTrans' exist and set flag */
set @TabcuOrdTransFlag = 0
if (exists (SELECT * FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_SCHEMA = schema_name() and  TABLE_NAME = 'cuOrdTrans'))
begin
	set @TabcuOrdTransFlag = 1
	/* Temp Table - plcusttran */
if object_id('tempdb.dbo.#tmp_cuOrdTrans', 'U') is not null
	drop table #tmp_cuOrdTrans;
	
	SELECT * into #tmp_cuOrdTrans FROM cuOrdTrans
									WHERE 1=1
									and itype = 7
									and PROPID = @PropID
									and TRANOrderNum = @stranOrdNum
									and tenantID = @tenantPer

end



						
/* If Order is Paid or Partially Paid then abort */
if (select count(*) from #tmp_OrderTrans WHERE bOpen = 0) > 0
	begin
		drop table #tmp_OrderTrans;
			if object_id('tempdb.dbo.#tmp_cuOrdTrans', 'U') is not null
				drop table #tmp_cuOrdTrans;
				Select 'Selected Order is Paid or Partially Paid. Only Unpaid Orders can be modified. Please go back and try again.'
				return
	end

end /* Create Temp table End */



set @CurrentChargeDate = (SELECT TOP 1 EventDate FROM #tmp_OrderTrans)
set @CurrentOrderDate = (SELECT TOP 1 DATE3 FROM #tmp_OrderTrans)

/** END Search for OrderTransaction */


begin
/** SELECT Action - Review Only */
/********************************/

if @OrderAction = 'Review'
begin
	SELECT 'Review Only' as Action_Type, TR.HMY, TR.HPARENT1
	,TR.PARENTID, TR.HPARENT3, TR.ITYPE
	,sOrderNum as System_Order_Numb, TRANOrderNum as Order_Numb
	,CtrlREFNumb as Control_Numb, convert(nvarchar(10), @CurrentChargeDate, 101) as CURRENT_Charge_Date
	,(case @FlChargeDateUpdate
			when 1 then  convert(nvarchar(10), @NewChargeDate, 101)
			when 0 then 'No change'
	end) as NEW_Charge_Date
	,convert(nvarchar(10), @CurrentOrderDate , 101) as CURRENT_Order_Date,
	(case @FlOrderDateUpdate
			when 1 then convert(nvarchar(10), @NewOrderDate, 101)
			when 0 then 'No change'
	end) as NEW_Order_Date
	,TR.PROPID, TR.STOTALAMOUNT, TR.HUNIT, TR.tenantID, TR.HACCRUALACCT
	FROM #tmp_OrderTrans TR
	ORDER BY CtrlREFNumb
end

/** UPDATE Action - Change Dates */
/*********************************/
else if @OrderAction = 'Update'
begin
	begin TRAN
/* UPDATE ChargeDate begin */
/*************************/
	if @FlChargeDateUpdate = 1
	begin

		/** Track changes insert to DMLHistoryTracker tab   **/
		/**************************************************/
		set @FixHistNote = 'Order correction(Script): Charge/Due Date'
		set @FixHistNote = @FixHistNote+'; Modified by: '+@sys_user

/*Insert to DMLHistoryTracker OrderTransaction update*/
		set @tr_cursor = cursor for
			SELECT hMy, EventDate FROM #tmp_OrderTrans
		open @tr_cursor;
			fetch next FROM @tr_cursor into @hMy, @EventDate
			while @@fetch_STATUS = 0

			begin
			/* DMLHistoryTracker OrderTransaction Update Insert actioned one by one in order to record all 'hmy' numbers */
				insert into DMLHistoryTracker (hyCRM, sTableName, hForeignKey, sNotes, sNewValue, sOldValue, dtDate, sColumnName)
				values (@hyCRM, 'trans', @hMy, @FixHistNote, convert(nvarchar(10), @NewChargeDate, 101),convert(nvarchar(10), @EventDate, 101), getdate(), 'EventDate')
				fetch next FROM @tr_cursor into @hMy, @EventDate
			end
		close @tr_cursor;
		deallocate @tr_cursor;

/* One collective update of OrderTransaction TABLE (not by hmy) to improve script efficiency */
		UPDATE OrderTransaction set EventDate = @NewChargeDate WHERE 1=1
		and itype = 7
		and PARENTID = @trParent2ID
		and sOrderNum = @sys_Ord_Num
		and PROPID = @PropID
		
		
/* UPDATE TABLE cuOrdTrans begin*/
if @TabcuOrdTransFlag = 1
	begin
	/*Insert to DMLHistoryTracker cuOrdTrans update*/
		set @plcu_cursor = cursor for
			SELECT hMy, EventDate FROM #tmp_cuOrdTrans
		open @plcu_cursor;
			fetch next FROM @plcu_cursor into @plcuhMy, @EventDate
			while @@fetch_STATUS = 0

			begin
			/* DMLHistoryTracker cuOrdTrans Update Insert actioned one by one in order to record all 'hmy' numbers */
				insert into DMLHistoryTracker (hyCRM, sTableName, hForeignKey, sNotes, sNewValue, sOldValue, dtDate, sColumnName)
				values (@hyCRM, 'cuOrdTrans', @plcuhMy, @FixHistNote, convert(nvarchar(10), @NewChargeDate, 101), convert(nvarchar(10), @EventDate, 101), getdate(), 'EventDate')
				fetch next FROM @plcu_cursor into @plcuhMy, @EventDate
			end
		close @plcu_cursor;
		deallocate @plcu_cursor;
/* One update of cuOrdTrans TABLE */
		UPDATE cuOrdTrans set EventDate = @NewChargeDate
		WHERE hmy IN (SELECT hmy FROM #tmp_cuOrdTrans)
	end
/* UPDATE TABLE cuOrdTrans end*/
end


/* UPDATE OrderDate begin */
/**************************/
IF @FlOrderDateUpdate = 1
begin

/* Track changes insert to DMLHistoryTracker tab  */
set @FixHistNote = 'Order correction(Script): Order Date'
set @FixHistNote = @FixHistNote+'; Modified by: '+@sys_user
set @tr_cursor = cursor for

	SELECT hMy, DATE3
	FROM #tmp_OrderTrans
	
	open @tr_cursor;
	fetch next FROM @tr_cursor into @hMy, @DATE3
	while @@fetch_STATUS = 0
		begin
			insert into DMLHistoryTracker (hyCRM, sTableName, hForeignKey, sNotes, sNewValue, sOldValue, dtDate, sColumnName)
			values (@hyCRM, 'trans', @hMy, @FixHistNote, convert(nvarchar(10), @NewOrderDate, 101), convert(nvarchar(10), @DATE3, 101), getdate(), 'DATE3')
			fetch next FROM @tr_cursor into @hMy, @DATE3
		end
			close @tr_cursor;
			deallocate @tr_cursor;
			
/*  UPDATE table OrderTransaction */
	UPDATE OrderTransaction set DATE3 = @NewOrderDate WHERE 1=1
	and itype = 7
	and PARENTID = @trParent2ID
	and sOrderNum = @sys_Ord_Num
	and PROPID = @PropID

/* UPDATE TABLE cuOrdTrans begin*/
if @TabcuOrdTransFlag = 1
	begin
	/*insert to DMLHistoryTracker cuOrdTrans update*/
		set @plcu_cursor = cursor for
		SELECT hMy, DATE3
		FROM #tmp_cuOrdTrans
		
		open @plcu_cursor;
		fetch next FROM @plcu_cursor into @plcuhMy, @DATE3
		while @@fetch_STATUS = 0
			begin
				/* DMLHistoryTracker cuOrdTrans Update insert actioned one by one in order to record all 'hmy' numbers */
				insert into DMLHistoryTracker (hyCRM, sTableName, hForeignKey, sNotes, sNewValue, sOldValue, dtDate, sColumnName)
				values (@hyCRM, 'cuOrdTrans', @plcuhMy, @FixHistNote, convert(nvarchar(10), @NewOrderDate, 101), convert(nvarchar(10), @DATE3, 101), getdate(), 'DATE3')
				fetch next FROM @plcu_cursor into @plcuhMy, @DATE3
			end
		close @plcu_cursor;
		deallocate @plcu_cursor;

		/* One update of cuOrdTrans TABLE */
		UPDATE cuOrdTrans set DATE3 = @NewOrderDate
		WHERE hmy IN (SELECT hmy FROM #tmp_cuOrdTrans)
	end
/* UPDATE TABLE cuOrdTrans end */
end
/* IF @FlOrderDateUpdate = 1 UPDATE OrderDate END */


/** Show updated changes ******/
/******************************/

SELECT
'Updated Record' as Action_Type, TR.HMY, TR.HPARENT1,TR.PARENTID, TR.HPARENT3, TR.ITYPE
,sOrderNum as System_Order_Numb, TRANOrderNum as Order_Numb, CtrlREFNumb as Control_Numb
,convert(nvarchar(10), @CurrentChargeDate, 101) as CURRENT_Charge_Date
,(case @FlChargeDateUpdate
	when 1 then  convert(nvarchar(10), @NewChargeDate, 101)
	when 0 then 'No change'
end) as NEW_Charge_Date
,convert(nvarchar(10), @CurrentOrderDate, 101) as CURRENT_Order_Date
,(case @FlOrderDateUpdate
	when 1 then convert(nvarchar(10), @NewOrderDate, 101)
	when 0 then 'No change'
end) as NEW_Order_Date
, TR.PROPID, TR.STOTALAMOUNT, TR.HUNIT, TR.tenantID, TR.HACCRUALACCT

FROM #tmp_OrderTrans TR
ORDER BY CtrlREFNumb

commit /* Commit Update TRAN */
end /* End Update sub-procedure */

else
SELECT 'Input data error'
end

drop table #tmp_OrderTrans;

if object_id('tempdb.dbo.#tmp_cuOrdTrans', 'U') is not null
	drop table #tmp_cuOrdTrans;

end;


end; /* procedure END */
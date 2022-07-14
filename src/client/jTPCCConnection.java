/*
 * jTPCCConnection
 *
 * One connection to the database. Used by either the old style
 * Terminal or the new TimedSUT.
 *
 * Copyright (C) 2004-2016, Denis Lussier
 * Copyright (C) 2016, Jan Wieck
 *
 */

import java.util.*;
import java.sql.*;

/**  
 * 准备 TPC-C SQL语句。用 PreparedStatement 加速。
 * https://github.com/domino-succ/tpcc-hbase/wiki/%E4%B8%AD%E6%96%87-TPC-C%E7%AE%80%E4%BB%8B
 */
public class jTPCCConnection
{
    private Connection          dbConn = null;
    private int                 dbType = 0;

    public PreparedStatement    stmtNewOrderSelectWhseCust;
    public PreparedStatement    stmtNewOrderSelectDist;
    public PreparedStatement    stmtNewOrderUpdateDist;
    public PreparedStatement    stmtNewOrderInsertOrder;
    public PreparedStatement    stmtNewOrderInsertNewOrder;
    public PreparedStatement    stmtNewOrderSelectStock;
    public PreparedStatement    stmtNewOrderSelectStockBatch[];
    public PreparedStatement    stmtNewOrderSelectItem;
    public PreparedStatement    stmtNewOrderSelectItemBatch[];
    public PreparedStatement    stmtNewOrderUpdateStock;
    public PreparedStatement    stmtNewOrderInsertOrderLine;

    public PreparedStatement    stmtPaymentSelectWarehouse;
    public PreparedStatement    stmtPaymentSelectDistrict;
    public PreparedStatement    stmtPaymentSelectCustomerListByLast;
    public PreparedStatement    stmtPaymentSelectCustomer;
    public PreparedStatement    stmtPaymentSelectCustomerData;
    public PreparedStatement    stmtPaymentUpdateWarehouse;
    public PreparedStatement    stmtPaymentUpdateDistrict;
    public PreparedStatement    stmtPaymentUpdateCustomer;
    public PreparedStatement    stmtPaymentUpdateCustomerWithData;
    public PreparedStatement    stmtPaymentInsertHistory;

    public PreparedStatement    stmtOrderStatusSelectCustomerListByLast;
    public PreparedStatement    stmtOrderStatusSelectCustomer;
    public PreparedStatement    stmtOrderStatusSelectLastOrder;
    public PreparedStatement    stmtOrderStatusSelectOrderLine;

    public PreparedStatement    stmtStockLevelSelectLow;

	public PreparedStatement    stmtDeliveryBGSelectOldestNewOrder;
    public PreparedStatement    stmtDeliveryBGDeleteOldestNewOrder;
    public PreparedStatement    stmtDeliveryBGSelectOrder;
    public PreparedStatement    stmtDeliveryBGUpdateOrder;
    public PreparedStatement    stmtDeliveryBGSelectSumOLAmount;
    public PreparedStatement    stmtDeliveryBGUpdateOrderLine;
    public PreparedStatement    stmtDeliveryBGUpdateCustomer;

    public jTPCCConnection(Connection dbConn, int dbType)
	throws SQLException
    {
	this.dbConn = dbConn;
	this.dbType = dbType;
	stmtNewOrderSelectStockBatch = new PreparedStatement[16];
	// 注意	sqlite 不支持 select for update.

	// 查询商品库存.
	// 注意TiDB新增, 有 in((?,?),(?,?),...)有16个.
	String st = "SELECT s_i_id, s_w_id, s_quantity, s_data, " +
				"       s_dist_01, s_dist_02, s_dist_03, s_dist_04, " +
				"       s_dist_05, s_dist_06, s_dist_07, s_dist_08, " +
				"       s_dist_09, s_dist_10 " +
				"    FROM bmsql_stock " +
				"    WHERE (s_w_id, s_i_id) in ((?,?)";
	for (int i = 1; i <= 15; i ++) {
		String stmtStr = st + ") FOR UPDATE"; // 上行锁
		stmtNewOrderSelectStockBatch[i] = dbConn.prepareStatement(stmtStr);
		st += ",(?,?)";
	}

	// 注意TiDB新增, 有 in((?,?),(?,?),...)有16个.
	stmtNewOrderSelectItemBatch = new PreparedStatement[16];
	st = "SELECT i_id, i_price, i_name, i_data " +
			"    FROM bmsql_item WHERE i_id in (?";
	for (int i = 1; i <= 15; i ++) {
		String stmtStr = st + ")";
		stmtNewOrderSelectItemBatch[i] = dbConn.prepareStatement(stmtStr);
		st += ",?";
	}

	/** PreparedStataments for NEW_ORDER */
	// 查询用户所在区的折扣与税务信息.
	// JOIN + 主键查询.
	// SELECT c_discount, c_last, c_credit, w_tax FROM bmsql_customer JOIN bmsql_warehouse ON (w_id = c_w_id) WHERE c_w_id = 1 AND c_d_id = 10 AND c_id = 346;
	stmtNewOrderSelectWhseCust = dbConn.prepareStatement(
		"SELECT c_discount, c_last, c_credit, w_tax " +
		"    FROM bmsql_customer " +
		"    JOIN bmsql_warehouse ON (w_id = c_w_id) " +
		"    WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?");

	// 在 bmsql_district 查询地区的税务与新订单id.
	// SELECT d_tax, d_next_o_id FROM bmsql_district WHERE d_w_id = 1 AND d_id = 1 FOR UPDATE
	stmtNewOrderSelectDist = dbConn.prepareStatement(
		"SELECT d_tax, d_next_o_id " +
		"    FROM bmsql_district " +
		"    WHERE d_w_id = ? AND d_id = ? " +
		"    FOR UPDATE");

	// 更新 bmsql_district 地区的新订单id.
	// UPDATE bmsql_district SET d_next_o_id = d_next_o_id + 1 WHERE d_w_id = ? AND d_id = ?;
	stmtNewOrderUpdateDist = dbConn.prepareStatement(
		"UPDATE bmsql_district " +
		"    SET d_next_o_id = d_next_o_id + 1 " +
		"    WHERE d_w_id = ? AND d_id = ?");

	// 在 bmsql_oorder 插入新订单.
	// INSERT INTO bmsql_oorder (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) VALUES (3001, 1, 1, 2571, '2022-07-04 10:49:25.439000', 11, 1);
	stmtNewOrderInsertOrder = dbConn.prepareStatement(
		"INSERT INTO bmsql_oorder (" +
		"    o_id, o_d_id, o_w_id, o_c_id, o_entry_d, " +
		"    o_ol_cnt, o_all_local) " +
		"VALUES (?, ?, ?, ?, ?, ?, ?)");

	// 在 bmsql_new_order 插入新订单.
	// INSERT INTO bmsql_new_order (no_o_id, no_d_id, no_w_id) VALUES (3001, 10, 1);
	stmtNewOrderInsertNewOrder = dbConn.prepareStatement(
		"INSERT INTO bmsql_new_order (" +
		"    no_o_id, no_d_id, no_w_id) " +
		"VALUES (?, ?, ?)");

	// 从 bmsql_stock 查询库存数.
	// SELECT s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 FROM bmsql_stock WHERE s_w_id = 1 AND s_i_id = 1853 FOR UPDATE;
	stmtNewOrderSelectStock = dbConn.prepareStatement(
		"SELECT s_quantity, s_data, " +
		"       s_dist_01, s_dist_02, s_dist_03, s_dist_04, " +
		"       s_dist_05, s_dist_06, s_dist_07, s_dist_08, " +
		"       s_dist_09, s_dist_10 " +
		"    FROM bmsql_stock " +
		"    WHERE s_w_id = ? AND s_i_id = ? " +
		"    FOR UPDATE");

	// 从 bmsql_item 查询商品价格等信息.
	// SELECT i_price, i_name, i_data FROM bmsql_item WHERE i_id = 15125;
	stmtNewOrderSelectItem = dbConn.prepareStatement(
		"SELECT i_price, i_name, i_data " +
		"    FROM bmsql_item " +
		"    WHERE i_id = ?");

	// 更新 bmsql_stock 更新商品库存 数量与s_ytd等信息.
	// UPDATE bmsql_stock SET s_quantity = 14, s_ytd = s_ytd + 3, s_order_cnt = s_order_cnt + 1, s_remote_cnt = s_remote_cnt + 0 WHERE s_w_id = 1 AND s_i_id = 2297;
	stmtNewOrderUpdateStock = dbConn.prepareStatement(
		"UPDATE bmsql_stock " +
		"    SET s_quantity = ?, s_ytd = s_ytd + ?, " +
		"        s_order_cnt = s_order_cnt + 1, " +
		"        s_remote_cnt = s_remote_cnt + ? " +
		"    WHERE s_w_id = ? AND s_i_id = ?");

	// 插入 bmsql_order_line 订单信息.
	// INSERT INTO bmsql_order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) VALUES (3001, 1, 1, 1, 1853, 1, 10, 294.20000000000005, 'nXfhvxeg1zADrUek6DS1PIDo');
	stmtNewOrderInsertOrderLine = dbConn.prepareStatement(
		"INSERT INTO bmsql_order_line (" +
		"    ol_o_id, ol_d_id, ol_w_id, ol_number, " +
		"    ol_i_id, ol_supply_w_id, ol_quantity, " +
		"    ol_amount, ol_dist_info) " +
		"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");

	/** PreparedStatements for PAYMENT */
	// 查询 bmsql_warehouse 仓库信息.
	// SELECT w_name, w_street_1, w_street_2, w_city, w_state, w_zip FROM bmsql_warehouse WHERE w_id = ?;
	stmtPaymentSelectWarehouse = dbConn.prepareStatement(
		"SELECT w_name, w_street_1, w_street_2, w_city, " +
		"       w_state, w_zip " +
		"    FROM bmsql_warehouse " +
		"    WHERE w_id = ? ");

	// 查询 bmsql_district 内仓库所有的地区信息.
	// SELECT d_name, d_street_1, d_street_2, d_city, d_state, d_zip FROM bmsql_district WHERE d_w_id = ? AND d_id = ?;
	stmtPaymentSelectDistrict = dbConn.prepareStatement(
		"SELECT d_name, d_street_1, d_street_2, d_city, " +
		"       d_state, d_zip " +
		"    FROM bmsql_district " +
		"    WHERE d_w_id = ? AND d_id = ?");

	// 在 bmsql_customer 根据用户名字查所有用户id.
	// SELECT c_id FROM bmsql_customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first;
	stmtPaymentSelectCustomerListByLast = dbConn.prepareStatement(
		"SELECT c_id " +
		"    FROM bmsql_customer " +
		"    WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? " +
		"    ORDER BY c_first");

	// 在 bmsql_customer 查询用户信息.
	// SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance FROM bmsql_customer WHERE c_w_id = 1 AND c_d_id = 5 AND c_id = 1163 FOR UPDATE;
	stmtPaymentSelectCustomer = dbConn.prepareStatement(
		"SELECT c_first, c_middle, c_last, c_street_1, c_street_2, " +
		"       c_city, c_state, c_zip, c_phone, c_since, c_credit, " +
		"       c_credit_lim, c_discount, c_balance " +
		"    FROM bmsql_customer " +
		"    WHERE c_w_id = ? AND c_d_id = ? AND c_id = ? " +
		"    FOR UPDATE");

	// 在 bmsql_customer 查询用户信息 c_data.
	// SELECT c_data FROM bmsql_customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;
	stmtPaymentSelectCustomerData = dbConn.prepareStatement(
		"SELECT c_data " +
		"    FROM bmsql_customer " +
		"    WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?");

	// 更新 bmsql_warehouse 的 w_ytd.
	// UPDATE bmsql_warehouse SET w_ytd = w_ytd + 2278.22 WHERE w_id = 1;
	stmtPaymentUpdateWarehouse = dbConn.prepareStatement(
		"UPDATE bmsql_warehouse " +
		"    SET w_ytd = w_ytd + ? " +
		"    WHERE w_id = ?");

	// 更新 bmsql_district 的 d_ytd.
	// UPDATE bmsql_district SET d_ytd = d_ytd + 2278.22 WHERE d_w_id = 1 AND d_id = 3;
	stmtPaymentUpdateDistrict = dbConn.prepareStatement(
		"UPDATE bmsql_district " +
		"    SET d_ytd = d_ytd + ? " +
		"    WHERE d_w_id = ? AND d_id = ?");

	// 更新 bmsql_customer 用户的支付信息 c_balance, c_ytd_payment等.
	// UPDATE bmsql_customer SET c_balance = c_balance - ?, c_ytd_payment = c_ytd_payment + ?, c_payment_cnt = c_payment_cnt + 1 WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;
	stmtPaymentUpdateCustomer = dbConn.prepareStatement(
		"UPDATE bmsql_customer " +
		"    SET c_balance = c_balance - ?, " +
		"        c_ytd_payment = c_ytd_payment + ?, " +
		"        c_payment_cnt = c_payment_cnt + 1 " +
		"    WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?");

	// 更新 bmsql_customer 用户的支付信息以及c_data  等.
	// UPDATE bmsql_customer SET c_balance = c_balance - ?, c_ytd_payment = c_ytd_payment + ?, c_payment_cnt = c_payment_cnt + 1, c_data = ?     WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;
	stmtPaymentUpdateCustomerWithData = dbConn.prepareStatement(
		"UPDATE bmsql_customer " +
		"    SET c_balance = c_balance - ?, " +
		"        c_ytd_payment = c_ytd_payment + ?, " +
		"        c_payment_cnt = c_payment_cnt + 1, " +
		"        c_data = ? " +
		"    WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?");

	// 在 bmsql_history 插入历史用户数据
	// INSERT INTO bmsql_history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data) VALUES (1747, 3, 1, 3, 1, '2022-07-04 10:49:27.660000', 2278.22, 't79oP7hw    e3eZAoPAdn');
	stmtPaymentInsertHistory = dbConn.prepareStatement(
		"INSERT INTO bmsql_history (" +
		"    h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, " +
		"    h_date, h_amount, h_data) " +
		"VALUES (?, ?, ?, ?, ?, ?, ?, ?)");

	/** PreparedStatements for ORDER_STATUS */
	// 在 bmsql_customer 根据用户名字查所有用户id.
	// SELECT c_id FROM bmsql_customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first;
	stmtOrderStatusSelectCustomerListByLast = dbConn.prepareStatement(
		"SELECT c_id " +
		"    FROM bmsql_customer " +
		"    WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? " +
		"    ORDER BY c_first");

	// 在 bmsql_customer 根据用户id查用户信息.
	// SELECT c_first, c_middle, c_last, c_balance FROM bmsql_customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;
	stmtOrderStatusSelectCustomer = dbConn.prepareStatement(
		"SELECT c_first, c_middle, c_last, c_balance " +
		"    FROM bmsql_customer " +
		"    WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?");

	// 在 bmsql_oorder 根据用户id查询最近的订单查询。
	// ORDER + LIMIT 键排序查询（单建排序）.
	// SELECT o_id, o_entry_d, o_carrier_id FROM bmsql_oorder WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ? AND o_id = (SELECT max(o_id) FROM bmsql_oorder WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ?);
	// TiDB有修改这个
	stmtOrderStatusSelectLastOrder = dbConn.prepareStatement(
		"SELECT o_id, o_entry_d, o_carrier_id " +
		"    FROM bmsql_oorder " +
		"    WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ? " +
		"      ORDER BY o_id DESC LIMIT 1");

	// 在 bmsql_order_line 查看单个订单信息.
	// SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM bmsql_order_line WHERE ol_w_id = 1 AND ol_d_id = 5 AND ol_o_id = 1163 ORDER BY ol_w_id, ol_d_id, ol_o_id, ol_number;
	stmtOrderStatusSelectOrderLine = dbConn.prepareStatement(
		"SELECT ol_i_id, ol_supply_w_id, ol_quantity, " +
		"       ol_amount, ol_delivery_d " +
		"    FROM bmsql_order_line " +
		"    WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? " +
		"    ORDER BY ol_w_id, ol_d_id, ol_o_id, ol_number");

	/** PreparedStatements for STOCK_LEVEL */
	switch (dbType)
	{
	    case jTPCCConfig.DB_POSTGRES:
	    case jTPCCConfig.DB_MYSQL:
		stmtStockLevelSelectLow = dbConn.prepareStatement(
		    "SELECT count(*) AS low_stock FROM (" +
		    "    SELECT s_w_id, s_i_id, s_quantity " +
		    "        FROM bmsql_stock " +
		    "        WHERE s_w_id = ? AND s_quantity < ? AND s_i_id IN (" +
		    "            SELECT /*+ TIDB_INLJ(bmsql_order_line) */ ol_i_id " +
		    "                FROM bmsql_district " +
		    "                JOIN bmsql_order_line ON ol_w_id = d_w_id " +
		    "                 AND ol_d_id = d_id " +
		    "                 AND ol_o_id >= d_next_o_id - 20 " +
		    "                 AND ol_o_id < d_next_o_id " +
		    "                WHERE d_w_id = ? AND d_id = ? " +
		    "        ) " +
		    "    ) AS L");
		break;

	    default:
		// 查询某地区内最近订单内s_quantity<?的商品数.
		// SELECT count(*) AS low_stock FROM (SELECT s_w_id, s_i_id, s_quantity FROM bmsql_stock WHERE s_w_id = 1 AND s_quantity < 19
		// AND s_i_id IN (SELECT ol_i_id FROM bmsql_district JOIN bmsql_order_line ON ol_w_id = d_w_id AND ol_d_id = d_id AND ol_o_id >= d_next_o_id - 20 AND ol_o_id < d_next_o_id WHERE d_w_id = 1 AND d_id = 4)) AS L;
		stmtStockLevelSelectLow = dbConn.prepareStatement(
		    "SELECT count(*) AS low_stock FROM (" +
		    "    SELECT s_w_id, s_i_id, s_quantity " +
		    "        FROM bmsql_stock " +
		    "        WHERE s_w_id = ? AND s_quantity < ? AND s_i_id IN (" +
		    "            SELECT ol_i_id " +
		    "                FROM bmsql_district " +
		    "                JOIN bmsql_order_line ON ol_w_id = d_w_id " +
		    "                 AND ol_d_id = d_id " +
		    "                 AND ol_o_id >= d_next_o_id - 20 " +
		    "                 AND ol_o_id < d_next_o_id " +
		    "                WHERE d_w_id = ? AND d_id = ? " +
		    "        ) " +
		    "    )");
		break;
	}


	/** PreparedStatements for DELIVERY_BG */
	// 查询 bmsql_new_order 中某地区的最旧订单.
	// SELECT no_o_id FROM bmsql_new_order WHERE no_w_id = ? AND no_d_id = ? ORDER BY no_o_id ASC
	// TiDB有修改这个加了 FOR UPDATE.
    stmtDeliveryBGSelectOldestNewOrder = dbConn.prepareStatement(
        "SELECT no_o_id " +
        "    FROM bmsql_new_order " +
        "    WHERE no_w_id = ? AND no_d_id = ? " +
        "    ORDER BY no_o_id ASC" +
        "    LIMIT 1" +
        "    FOR UPDATE");
	// DELETE FROM bmsql_new_order WHERE no_w_id = ? AND no_d_id = ? AND no_o_id = ?;
	// 注意TiDB有修改这个, 这里有10个(?,?,?).
	// 注意 sqlite 不允许这种判断多列是否在数组的语法 (c0,c1) in ((?,?)).
	stmtDeliveryBGDeleteOldestNewOrder = dbConn.prepareStatement(
		"DELETE FROM bmsql_new_order " +
		"    WHERE (no_w_id,no_d_id,no_o_id) IN (" +
		"(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?)," +
		"(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?))");

	// 从 bmsql_oorder 查找单个订单信息.
	// SELECT o_c_id FROM bmsql_oorder WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?;
	// 注意TiDB有修改这个, 这里有10个(?,?,?).
	stmtDeliveryBGSelectOrder = dbConn.prepareStatement(
		"SELECT o_c_id, o_d_id" +
		"    FROM bmsql_oorder " +
		"    WHERE (o_w_id,o_d_id,o_id) IN (" +
		"(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?)," +
		"(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?))");

	// 更新 bmsql_oorder 单个订单的的 o_carrier_id
	// UPDATE bmsql_oorder SET o_carrier_id = ? WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?;
	// 注意TiDB有修改这个, 这里有10个(?,?,?).
	stmtDeliveryBGUpdateOrder = dbConn.prepareStatement(
		"UPDATE bmsql_oorder " +
		"    SET o_carrier_id = ? " +
		"    WHERE (o_w_id,o_d_id,o_id) IN (" +
		"(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?)," +
		"(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?))");
    
	// 在 bmsql_order_line 查询同为 ol_o_id 的订单的总量 ol_amount.
	// SELECT sum(ol_amount) AS sum_ol_amount FROM bmsql_order_line WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?;
	// 注意TiDB有修改这个, 这里有10个(?,?,?).
	stmtDeliveryBGSelectSumOLAmount = dbConn.prepareStatement(
		"SELECT sum(ol_amount) AS sum_ol_amount, ol_d_id" +
		"    FROM bmsql_order_line " +
		"    WHERE (ol_w_id,ol_d_id,ol_o_id) IN (" +
		"(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?)," +
		"(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?)" +
		") GROUP BY ol_d_id");

    // 在 bmsql_order_line 更新 ol_o_id 订单的 ol_delivery_d.
	// UPDATE bmsql_order_line SET ol_delivery_d = ? WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?;
	// 注意TiDB有修改这个, 这里有10个(?,?,?).
	stmtDeliveryBGUpdateOrderLine = dbConn.prepareStatement(
		"UPDATE bmsql_order_line " +
		"    SET ol_delivery_d = ? " +
		"    WHERE (ol_w_id,ol_d_id,ol_o_id) IN (" +
		"(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?)," +
		"(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?))");

	// 在 bmsql_customer 更新单个用户的 c_balance 与 c_delivery_cnt.
	// UPDATE bmsql_customer SET c_balance = c_balance + ?, c_delivery_cnt = c_delivery_cnt + 1 WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?
	stmtDeliveryBGUpdateCustomer = dbConn.prepareStatement(
		"UPDATE bmsql_customer " +
		"    SET c_balance = c_balance + ?, " +
		"        c_delivery_cnt = c_delivery_cnt + 1 " +
		"    WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?");
    }

    public jTPCCConnection(String connURL, Properties connProps, int dbType)
	throws SQLException
    {
	this(DriverManager.getConnection(connURL, connProps), dbType);
    }

    public void commit()
	throws SQLException
    {
    	try {
			dbConn.commit();
		} catch(SQLException e) {
    		throw new CommitException();
		}
    }

    public void rollback()
	throws SQLException
    {
	dbConn.rollback();
    }
}

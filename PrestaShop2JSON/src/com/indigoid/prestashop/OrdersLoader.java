package com.indigoid.prestashop;

/**
 * Loads orders data into memory from the database manager. The orders to be
 * loaded are those making the predicate true.
 * 
 * @author Charlie
 *
 */
public class OrdersLoader extends PrstshpLoader {

	/**
	 * document_type JSON property value for this kind of objects
	 */
	private static final String ORDERS_DOC_TYPE = "order";
	/**
	 * JSON property for combination attributes property
	 */
	private static final String COMBINATION_ATTRIBUTES_PROPERTY = "combination_attributes";
	/**
	 * JSON property for payments property
	 */
	private static final String PAYMENTS_PROPERTY = "payments";
	/**
	 * JSON property for employee property
	 */
	private static final String EMPLOYEE_PROTERY = "employee";
	/**
	 * JSON property for order history property
	 */
	private static final String HISTORY_PROPERTY = "history";
	/**
	 * JSON property for order details property
	 */
	private static final String DETAILS_PROPERTY = "details";
	/**
	 * JSON property for invoice property
	 */
	private static final String INVOICE_PROPERTY = "invoice";
	/**
	 * JSON property for discounts property
	 */
	private static final String DISCOUNTS_PROPETRY = "discounts";
	/**
	 * JSON property for carrier property
	 */
	private static final String CARRIER_PROPERTY = "carrier";
	/**
	 * JSON property for invoice address property
	 */
	private static final String INVOICE_ADDRESS_PROPERTY = "invoice_address";
	/**
	 * JSON property for delivery address property
	 */
	private static final String DELIVERY_ADDRESS_PROPERTY = "delivery_address";
	/**
	 * JSON property for customer property
	 */
	private static final String CUSTOMER_PROPERTY = "customer";
	/**
	 * JSON property for customer thread messages property
	 */
	private static final String THREAD_MESSAGES_PROPERTY = "thread_messages";
	/**
	 * JSON property for customer thread property
	 */
	private static final String CUSTOMER_THREAD_PROPERTY = "customer_thread";
	/**
	 * JSON property for order messages property
	 */
	private static final String MESSAGES_PROPERTY = "messages";
	/**
	 * JSON property for order slips details property
	 */
	private static final String CREDIT_SLIPS_DETAILS_PROPERTY = "credit_slips_details";
	/**
	 * JSON property for order slips property
	 */
	private static final String CREDIT_SLIPS_PROPERTY = "credit_slips";
	/**
	 * JSON property for returns details property
	 */
	private static final String RETURN_DETAILS_PROPERTY = "return_details";
	/**
	 * JSON property for returns property
	 */
	private static final String RETURNS_PROPERTY = "returns";	
	/**
	 * Key column for the product combinations table (prstshp_product_attribute)
	 */
	private static final String PRODUCT_COMBINATION_KEY_COLUMN = "id_product_attribute";
	/**
	 * Key column for the orders table
	 */
	private static final String ORDERS_KEY_COLUMN = "id_order";
	/**
	 * Key column for the employees table
	 */
	private static final String EMPLOYEES_KEY_COLUMN = "id_employee";
	/**
	 * Key column for the order invoices table
	 */
	private static final String INVOICES_KEY_COLUMN = "id_order_invoice";
	/**
	 * Key column for the customer threads table
	 */
	private static final String CUSTOMER_THREAD_KEY_COLUMN = "id_customer_thread";
	/**
	 * Key column for the order slips table
	 */
	private static final String ORDER_SLIP_KEY_COLUMN = "id_order_slip";
	/**
	 * Key column for the order returns table
	 */
	private static final String ORDER_RETURN_KEY_COLUMN = "id_order_return";
	/**
	 * This is a special type of tax that may apply at the invoice level.
	 * Cardinality 0:1
	 */
	private static final String SPECIAL_TAXABLE_INVOICE_CONCEPT = "\"wrapping\"";
	/**
	 * Select statement to retrieve Orders data. `secure_key` is not retrieved.
	 */
	private static final String GET_ORDERS_QUERY = "SELECT t1.id_order, t1.reference, t1.id_shop_group, t1.id_shop, t1.id_lang, t3.name as lang, "
			+ "t1.id_cart, t1.id_currency, t2.name as currency, t2.iso_code as currency_iso_code, t1.current_state, "
			+ "t4.name as current_state_name, t1.payment, t1.conversion_rate, t1.module, t1.recyclable, t1.gift, t1.gift_message, t1.mobile_theme, "
			+ "t1.shipping_number, t1.total_discounts, t1.total_discounts_tax_incl, t1.total_discounts_tax_excl, t1.total_paid, t1.total_paid_tax_incl, "
			+ "t1.total_paid_tax_excl, t1.total_paid_real, t1.total_products, t1.total_products_wt, t1.total_shipping, t1.total_shipping_tax_incl, "
			+ "t1.total_shipping_tax_excl, t1.carrier_tax_rate, t1.total_wrapping, t1.total_wrapping_tax_incl, t1.total_wrapping_tax_excl, "
			+ "t1.round_mode, t1.round_type, t1.invoice_number, t1.delivery_number, t1.invoice_date, t1.delivery_date, t1.valid, t1.date_add, t1.date_upd "
			+ "FROM prstshp_orders AS t1 " + "JOIN prstshp_currency AS t2 ON t1.id_currency = t2.id_currency "
			+ "JOIN prstshp_lang AS t3 ON t1.id_lang = t3.id_lang "
			+ "JOIN prstshp_order_state_lang AS t4 ON t1.current_state = t4.id_order_state AND t1.id_lang = t4.id_lang ";
	/**
	 * Select statement to retrieve the delivery address for an order. The statement
	 * to retrieve the invoice address is identical except for
	 * t1.id_address_delivery which should be t1.id_address_invoice
	 */
	private static final String GET_DELIVERY_ADDRESS_QUERY = "SELECT t2.id_address, t2.id_country, t3.name as country, t2.id_state, t4.name as state, "
			+ "t2.id_manufacturer, t2.id_supplier, t2.id_warehouse, t2.alias, t2.company, t2.lastname, t2.firstname, t2.address1, t2.address2, t2.postcode, "
			+ "t2.city, t2.other, t2.phone, t2.phone_mobile, t2.vat_number, t2.dni, t2.date_add, t2.date_upd, t2.active, t2.deleted "
			+ "FROM prstshp_orders AS t1 " + "JOIN prstshp_address AS t2 ON t1.id_address_delivery = t2.id_address "
			+ "JOIN prstshp_country_lang AS t3 ON t2.id_country = t3.id_country AND t1.id_lang = t3.id_lang "
			+ "JOIN prstshp_state AS t4 ON t2.id_state = t4.id_state " + "WHERE id_order = ?";
	/**
	 * The statement to retrieve the invoice address is identical to
	 * GET_DELIVERY_ADDRESS_QUERY except for "t1.id_address_delivery" which should
	 * be "t1.id_address_invoice". However a second condition is added to prevent
	 * repeating the address when invoice and delivery addresses are the same.
	 */
	private static final String GET_INVOICE_ADDRESS_QUERY = GET_DELIVERY_ADDRESS_QUERY.replace(
			"_delivery = t2.id_address",
			"_invoice = t2.id_address AND t1.id_address_delivery <> t1.id_address_invoice");
	/**
	 * Select statement to retrieve the carrier of an order.
	 */
	private static final String GET_CARRIER_QUERY = "SELECT t1.id_carrier, t2.name, t1.id_order_invoice, t1.weight, "
			+ "t1.shipping_cost_tax_excl, t1.shipping_cost_tax_incl, t1.tracking_number, t1.date_add "
			+ "FROM prstshp_order_carrier AS t1 " + "JOIN prstshp_carrier AS t2 ON t1.id_carrier = t2.id_carrier "
			+ "WHERE id_order = ?";
	/**
	 * Select statement to retrieve the discounts applied to a particular order
	 */
	private static final String GET_DISCOUNTS_QUERY = "SELECT t1.id_cart_rule, t1.id_order_invoice, t1.name, t1.value, t1.value_tax_excl, "
			+ "t1.free_shipping, t2.id_customer, t2.date_from, t2.date_to, t2.description, t2.quantity, t2.quantity_per_user, t2.priority, "
			+ "t2.partial_use, t2.code, t2.minimum_amount, t2.minimum_amount_tax, t2.minimum_amount_currency, t2.minimum_amount_shipping, "
			+ "t2.country_restriction, t2.carrier_restriction, t2.group_restriction, t2.cart_rule_restriction, t2.product_restriction, "
			+ "t2.shop_restriction, t2.free_shipping, t2.reduction_percent, t2.reduction_amount, t2.reduction_tax, t2.reduction_currency, "
			+ "t3.name as reduction_currency_name, t3.iso_code as reduction_currency_iso_code, t2.reduction_product, t2.gift_product, "
			+ "t2.gift_product_attribute, t2.highlight, t2.active, t2.date_add, t2.date_upd "
			+ "FROM prstshp_order_cart_rule AS t1 "
			+ "JOIN prstshp_cart_rule AS t2 ON t1.id_cart_rule = t2.id_cart_rule "
			+ "JOIN prstshp_currency AS t3 ON t2.reduction_currency = t3.id_currency " + "WHERE id_order = ?";
	/**
	 * Select statement to retrieve the basic customer information
	 */
	private static final String GET_CUSTOMER_QUERY = "SELECT t1.id_customer, t1.id_gender, t2.name as gender, t1.id_default_group, "
			+ "t4.name as default_group, t1.id_lang, t3.name as lang, t1.firstname, t1.lastname, t1.email "
			+ "FROM prstshp_customer AS t1 "
			+ "LEFT OUTER JOIN prstshp_gender_lang AS t2 ON t1.id_gender = t2.id_gender AND t1.id_lang = t2.id_lang "
			+ "INNER JOIN prstshp_lang AS t3 ON t1.id_lang = t3.id_lang "
			+ "INNER JOIN prstshp_group_lang AS t4 ON t1.id_default_group = t4.id_group AND t1.id_lang = t4.id_lang "
			+ "WHERE t1.id_customer = (SELECT id_customer FROM prstshp_orders WHERE id_order = ?)";
	/**
	 * Select statement to retrieve the order lines on the order
	 */
	private static final String GET_ORDER_DETAILS_QUERY = "SELECT t1.id_order_detail, t1.id_order_invoice, t1.id_warehouse, t1.id_shop, "
			+ "t1.product_id, t1.product_attribute_id as id_product_attribute, t1.product_name, t1.product_quantity, t1.product_quantity_in_stock, "
			+ "t1.product_quantity_refunded, t1.product_quantity_return, t1.product_quantity_reinjected, t1.product_price, t1.reduction_percent, "
			+ "t1.reduction_amount, t1.reduction_amount_tax_incl, t1.reduction_amount_tax_excl, t1.group_reduction, t1.product_quantity_discount, "
			+ "t1.product_ean13, t1.product_upc, t1.product_reference, t1.product_supplier_reference, t1.product_weight, t1.id_tax_rules_group, "
			+ "t1.tax_computation_method, t4.rate as tax_rate, t5.name as tax_name, t3.unit_amount as tax_unit_amount, t3.total_amount as tax_total_amount, "
			+ "t1.ecotax, t1.ecotax_tax_rate, t1.discount_quantity_applied, t1.download_hash, "
			+ "t1.download_nb, t1.download_deadline, t1.total_price_tax_incl, t1.total_price_tax_excl, t1.unit_price_tax_incl, t1.unit_price_tax_excl, "
			+ "t1.total_shipping_price_tax_incl, t1.total_shipping_price_tax_excl, t1.purchase_supplier_price, t1.original_product_price, "
			+ "t1.original_wholesale_price " + "FROM prstshp_order_detail AS t1 "
			+ "INNER JOIN prstshp_orders AS t2 ON t1.id_order = t2.id_order "
			+ "INNER JOIN prstshp_order_detail_tax AS t3 ON t1.id_order_detail = t3.id_order_detail "
			+ "INNER JOIN prstshp_tax AS t4 ON t3.id_tax = t4.id_tax "
			+ "INNER JOIN prstshp_tax_lang AS t5 ON t4.id_tax = t5.id_tax AND t2.id_lang = t5.id_lang "
			+ "WHERE t1.id_order = ? " + "ORDER BY t1.id_order_detail";
	/**
	 * Statement used to retrieve the attributes of a particular combination
	 * (id_product_attribute)
	 */
	private static final String GET_PRODUCT_COMBINATION_ATTRIBUTES_QUERY = "SELECT t3.id_lang, t5.name as lang, t1.id_attribute,  t2.color, t2.position,  "
			+ "t2.id_attribute_group, t3.name, t3.public_name, t4.name as value "
			+ "FROM prstshp_product_attribute_combination AS t1 "
			+ "JOIN prstshp_attribute AS t2 ON t1.id_attribute = t2.id_attribute "
			+ "JOIN prstshp_attribute_group_lang AS t3 ON t2.id_attribute_group = t3.id_attribute_group "
			+ "JOIN prstshp_attribute_lang AS t4 ON t1.id_attribute = t4.id_attribute "
			+ "JOIN prstshp_lang AS T5 ON t3.id_lang = t5.id_lang AND t4.id_lang = t5.id_lang "
			+ "WHERE t1.id_product_attribute = ? " + "ORDER BY t3.id_lang, t2.id_attribute_group";
	/**
	 * Select statement to retrieve the invoice associated to an order
	 */
	private static final String GET_INVOICE_QUERY = "SELECT t2.id_order_invoice, t2.id_order, t2.number, t2.delivery_number, t2.delivery_date, "
			+ "t2.total_discount_tax_excl, t2.total_discount_tax_incl, t2.total_paid_tax_excl, t2.total_paid_tax_incl, t2.total_products, "
			+ "t2.total_products_wt, t2.total_shipping_tax_excl, t2.total_shipping_tax_incl, t2.shipping_tax_computation_method, "
			+ "t2.total_wrapping_tax_excl, t2.total_wrapping_tax_incl, "
			+ "t3.id_tax as id_wrapping_tax, t3.amount as wrapping_tax_amount, t4.rate as wrapping_tax_rate, t5.name as wrapping_tax_name, "
			+ "t2.shop_address, t2.invoice_address, t2.delivery_address, t2.note, t2.date_add "
			+ "FROM prstshp_order_invoice AS t2 "
			+ "LEFT JOIN prstshp_order_invoice_tax AS t3 ON t2.id_order_invoice = t3.id_order_invoice AND t3.`type` = "
			+ SPECIAL_TAXABLE_INVOICE_CONCEPT + " " + "LEFT JOIN prstshp_tax AS t4 ON t3.id_tax = t4.id_tax "
			+ "LEFT JOIN prstshp_tax_lang AS t5 ON t4.id_tax = t5.id_tax AND t5.id_lang = (SELECT id_lang FROM prstshp_orders WHERE id_order = t2.id_order) "
			+ "WHERE t2.id_order_invoice IN (SELECT id_order_invoice FROM prstshp_order_detail WHERE id_order = ?)";
	/**
	 * Select statement to retrieve the payments done over a invoice
	 */
	private static final String GET_INVOICE_PAYMENTS_QUERY = "SELECT t2.id_order_payment, t2.order_reference, t2.id_currency, t3.name as currency_name, "
			+ "t3.iso_code as currency_iso_Code, t2.amount, t2.payment_method, t2.conversion_rate, t2.transaction_id, t2.card_number, t2.card_brand, "
			+ "t2.card_expiration, t2.card_holder, t2.date_add " + "FROM prstshp_order_invoice_payment  AS t1 "
			+ "INNER JOIN prstshp_order_payment AS t2 ON t1.id_order_payment = t2.id_order_payment "
			+ "INNER JOIN prstshp_currency AS t3 ON t2.id_currency = t3.id_currency " + "WHERE t1.id_order_invoice = ?";
	/**
	 * Select statement to retrieve the history of order statuses
	 */
	private static final String GET_ORDER_HISTORY_QUERY = "SELECT t2.id_order_history, t2.id_employee, t2.id_order_state, t3.name as state_name, t2.date_add "
			+ "FROM prstshp_orders AS t1 " + "INNER JOIN prstshp_order_history AS t2 ON t1.id_order = t2.id_order "
			+ "INNER JOIN prstshp_order_state_lang AS t3 ON t2.id_order_state = t3.id_order_state AND t1.id_lang = t3.id_lang "
			+ "WHERE t1.id_order = ? " + "ORDER BY t2.id_order_history";
	/**
	 * Select statement to retrieve the basic data of an employee
	 */
	private static final String GET_EMPLOYEE_QUERY = "SELECT t1.id_employee, t1.id_profile, t2.name as profile_name, t1.id_lang, t1.lastname, t1.firstname, t1.email "
			+ "FROM prstshp_employee AS t1 "
			+ "INNER JOIN prstshp_profile_lang AS t2 ON t1.id_profile = t2.id_profile AND t1.id_lang = t2.id_lang "
			+ "WHERE id_employee = ?";
	/**
	 * Select statement to retrieve order slips
	 */
	private static final String GET_ORDER_SLIPS_QUERY = "SELECT id_order_slip, conversion_rate, id_customer, total_products_tax_excl, total_products_tax_incl, "
			+ "total_shipping_tax_excl, total_shipping_tax_incl, shipping_cost, amount, shipping_cost_amount, `partial`, order_slip_type, date_add, date_upd "
			+ "FROM prstshp_order_slip " + "WHERE id_order = ?";
	/**
	 * Select statement to retrieve order slip details
	 */
	private static final String GET_ORDER_SLIP_DETAILS_QUERY = "SELECT id_order_detail, product_quantity, unit_price_tax_excl, unit_price_tax_incl, "
			+ "total_price_tax_excl, total_price_tax_incl, amount_tax_excl, amount_tax_incl "
			+ "FROM prstshp_order_slip_detail " + "WHERE id_order_slip = ?";
	/**
	 * Select statement to retrieve order returns
	 */
	private static final String GET_ORDER_RETURNS_QUERY = "SELECT t1.id_order_return, t1.state, t2.name as state_name, t1.question, t1.date_add, t1.date_upd "
			+ "FROM prstshp_order_return AS t1 "
			+ "INNER JOIN prstshp_order_return_state_lang AS t2 ON t2.id_order_return_state = t1.state AND t2.id_lang = (SELECT id_lang FROM prstshp_orders WHERE id_order = t1.id_order) "
			+ "WHERE id_order = ?";
	/**
	 * Select statement to retrieve order return details
	 */
	private static final String GET_ORDER_RETURN_DETAILS_QUERY = "SELECT id_order_detail, id_customization, product_quantity "
			+ "FROM prstshp_order_return_detail " + "WHERE id_order_return = ?";
	/**
	 * Select statement to retrieve order messages
	 */
	private static final String GET_MESSAGES_QUERY = "SELECT id_message, message, private, date_add "
			+ "FROM prstshp_message " + "WHERE id_order = ? " + "ORDER BY id_message";
	/**
	 * Select statement to retrieve the customer thread (e-mails)
	 */
	private static final String GET_CUSTOMER_THREAD_QUERY = "SELECT t1.id_customer_thread, t1.id_shop, t1.id_lang, t4.name as lang, t1.id_contact, "
			+ "t2.email as contact_email, t2.customer_service as contact_customer_service, t2.position as contact_position, t3.name as contact_name, "
			+ "t3.description as contact_description, t1.id_product, t1.`status`, t1.email as from_email, t1.date_add, t1.date_upd "
			+ "FROM prstshp_customer_thread AS t1 "
			+ "INNER JOIN prstshp_contact AS t2 ON t2.id_contact = t1.id_contact "
			+ "INNER JOIN prstshp_contact_lang AS t3 ON t3.id_contact = t1.id_contact AND t3.id_lang = t1.id_lang "
			+ "INNER JOIN prstshp_lang AS t4 ON t4.id_lang = t1.id_lang " + "WHERE id_order = ?";
	/**
	 * Select statement to retrieve customer thread e-mails
	 */
	private static final String GET_THREAD_MESSAGES_QUERY = "SELECT id_customer_message, id_employee, message, file_name, ip_address, user_agent, "
			+ "date_add, date_upd, private, `read` " + "FROM prstshp_customer_message "
			+ "WHERE id_customer_thread = ?";

	/**
	 * Creates a loader for all orders.
	 */
	public OrdersLoader() {
		super();
	}

	/**
	 * Creates a loader for orders fulfilling a particular predicate
	 * 
	 * @param predicate
	 *            Condition that the selected orders must follow.
	 */
	public OrdersLoader(String predicate) {
		super(predicate);
	}

	/**
	 * Load orders information from a PrestaShop database.
	 * 
	 * @param con
	 *            The database manager connection
	 * @param shopName
	 *            Name of the shop to identify generated top-level documents
	 * @return The number of orders loaded.
	 */
	public int load(com.indigoid.dbutils.MariaDBConnectionManager con, String shopName) {
		// These are the selectors for child elements
		DataSelector[][] childSelectors = {
				{ new DataSelector(ORDERS_KEY_COLUMN, CUSTOMER_PROPERTY, GET_CUSTOMER_QUERY, false) },
				{ new DataSelector(ORDERS_KEY_COLUMN, DELIVERY_ADDRESS_PROPERTY, GET_DELIVERY_ADDRESS_QUERY, false) },
				{ new DataSelector(ORDERS_KEY_COLUMN, INVOICE_ADDRESS_PROPERTY, GET_INVOICE_ADDRESS_QUERY, false) },
				{ new DataSelector(ORDERS_KEY_COLUMN, CARRIER_PROPERTY, GET_CARRIER_QUERY, false) },
				{ new DataSelector(ORDERS_KEY_COLUMN, DISCOUNTS_PROPETRY, GET_DISCOUNTS_QUERY, true) },
				{ new DataSelector(ORDERS_KEY_COLUMN, INVOICE_PROPERTY, GET_INVOICE_QUERY, false),
						new DataSelector(INVOICES_KEY_COLUMN, PAYMENTS_PROPERTY, GET_INVOICE_PAYMENTS_QUERY, true) },
				{ new DataSelector(ORDERS_KEY_COLUMN, DETAILS_PROPERTY, GET_ORDER_DETAILS_QUERY, true),
						new DataSelector(PRODUCT_COMBINATION_KEY_COLUMN, COMBINATION_ATTRIBUTES_PROPERTY,
								GET_PRODUCT_COMBINATION_ATTRIBUTES_QUERY, true) },
				{ new DataSelector(ORDERS_KEY_COLUMN, HISTORY_PROPERTY, GET_ORDER_HISTORY_QUERY, true),
						new DataSelector(EMPLOYEES_KEY_COLUMN, EMPLOYEE_PROTERY, GET_EMPLOYEE_QUERY, false) },
				{ new DataSelector(ORDERS_KEY_COLUMN, RETURNS_PROPERTY, GET_ORDER_RETURNS_QUERY, true),
						new DataSelector(ORDER_RETURN_KEY_COLUMN, RETURN_DETAILS_PROPERTY, GET_ORDER_RETURN_DETAILS_QUERY, true) },
				{ new DataSelector(ORDERS_KEY_COLUMN, CREDIT_SLIPS_PROPERTY, GET_ORDER_SLIPS_QUERY, true),
						new DataSelector(ORDER_SLIP_KEY_COLUMN, CREDIT_SLIPS_DETAILS_PROPERTY, GET_ORDER_SLIP_DETAILS_QUERY, true) },
				{ new DataSelector(ORDERS_KEY_COLUMN, MESSAGES_PROPERTY, GET_MESSAGES_QUERY, true),
						new DataSelector(EMPLOYEES_KEY_COLUMN, EMPLOYEE_PROTERY, GET_EMPLOYEE_QUERY, false) },
				{ new DataSelector(ORDERS_KEY_COLUMN, CUSTOMER_THREAD_PROPERTY, GET_CUSTOMER_THREAD_QUERY, false),
						new DataSelector(CUSTOMER_THREAD_KEY_COLUMN, THREAD_MESSAGES_PROPERTY, GET_THREAD_MESSAGES_QUERY, true),
						new DataSelector(EMPLOYEES_KEY_COLUMN, EMPLOYEE_PROTERY, GET_EMPLOYEE_QUERY, false) } };

		return loadMainLevel(con, shopName, new DataSelector(ORDERS_KEY_COLUMN, ORDERS_DOC_TYPE, GET_ORDERS_QUERY, false),
				childSelectors);
	}
}
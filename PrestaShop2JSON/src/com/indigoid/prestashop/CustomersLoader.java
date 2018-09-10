package com.indigoid.prestashop;

/**
 * Loads customer data into memory from the database manager. The customer to be
 * loaded are those making the predicate true.
 * 
 * @author Charlie
 *
 */
public class CustomersLoader extends PrstshpLoader {

	/**
	 * document_type JSON property value for this kind of objects
	 */
	private static final String CUSTOMERS_DOC_TYPE = "customer";
	/**
	 * Key column for the customers table
	 */
	private static final String CUSTOMERS_KEY_COLUMN = "id_customer";
	/**
	 * JSON property for customer groups
	 */
	private static final String GROUPS_PROPERTY = "groups";
	/**
	 * JSON property for customer address
	 */
	private static final String ADDRESSES_PROPERTY = "addresses";
	/**
	 * Select statement to retrieve Customers data. passwd and secure_key fields
	 * not retrieved for security reasons.
	 */
	private static final String GET_CUSTOMERS_QUERY = "SELECT t1.id_customer, t1.id_shop_group, t1.id_shop, t1.id_gender, t2.name as gender, "
			+ "t1.id_default_group, t4.name as default_group, t1.id_lang, t3.name as lang, t1.id_risk, t5.name as risk_description,  "
			+ "t1.company, t1.siret, t1.ape, t1.firstname, t1.lastname, t1.email, t1.last_passwd_gen, t1.birthday, t1.newsletter, "
			+ "t1.ip_registration_newsletter, t1.newsletter_date_add, t1.optin, t1.website, t1.outstanding_allow_amount, "
			+ "t1.show_public_prices, t1.max_payment_days, t1.note, t1.active, t1.is_guest, t1.deleted, t1.date_add, t1.date_upd "
			+ "FROM prstshp_customer AS t1 "
			+ "LEFT OUTER JOIN prstshp_gender_lang AS t2 ON t1.id_gender = t2.id_gender AND t1.id_lang = t2.id_lang "
			+ "INNER JOIN prstshp_lang AS t3 ON t1.id_lang = t3.id_lang "
			+ "INNER JOIN prstshp_group_lang AS t4 ON t1.id_default_group = t4.id_group AND t1.id_lang = t4.id_lang "
			+ "LEFT OUTER JOIN prstshp_risk_lang AS t5 ON t1.id_risk = t5.id_risk AND t1.id_lang = t5.id_lang ";
	
	/**
	 * Select statement used to retrieve the delivery addresses for a given
	 * customer.
	 */
	private static final String GET_CUSTOMER_ADDRESSES_QUERY = "SELECT id_address, id_country, id_state, id_customer, "
			+ "id_manufacturer, id_supplier, id_warehouse, alias, company, lastname, firstname, address1, "
			+ "address2, postcode, city, other, phone, phone_mobile, vat_number, dni, date_add, date_upd, "
			+ "active, deleted " + "FROM prstshp_address WHERE id_customer = ?";
	/**
	 * Statement used to retrieve the group names assigned to a user (in all
	 * languages)
	 */
	private static final String GET_CUSTOMERS_GROUPS_QUERY = "SELECT t1.id_group, t2.id_lang, t3.name as lang, t2.name as group_name "  
			+ "FROM prstshp_customer_group AS t1 " 
			+ "INNER JOIN prstshp_group_lang AS t2 ON t1.id_group = t2.id_group " 
			+ "INNER JOIN prstshp_lang AS t3 ON t2.id_lang = t3.id_lang " 
			+ "WHERE t1.id_customer = ? AND t2.id_lang = (SELECT t4.id_default_group FROM prstshp_customer AS t4 WHERE t4.id_customer = t1.id_customer) " 
			+ "ORDER BY t1.id_group";
	/**
	 * Creates a loader for all customers.
	 */
	public CustomersLoader() {
		super();
	}

	/**
	 * Creates a loader for customers fulfilling a particular predicate
	 * 
	 * @param predicate
	 *            Condition that the selected customers must follow.
	 */
	public CustomersLoader(String predicate) {
		super(predicate);
	}

	/**
	 * Load customers information from a PrestaShop database.
	 * 
	 * @param con
	 *            The database manager connection
	 * @param shopName
	 *            Name of the shop to identify generated top-level documents
	 * @return The number of customers loaded.
	 */
	public int load(com.indigoid.dbutils.MariaDBConnectionManager con, String shopName) {
		// These are the selectors for child elements
		DataSelector[][] childSelectors = {	
				{new DataSelector(CUSTOMERS_KEY_COLUMN, ADDRESSES_PROPERTY, GET_CUSTOMER_ADDRESSES_QUERY, true)},
				{new DataSelector(CUSTOMERS_KEY_COLUMN, GROUPS_PROPERTY, GET_CUSTOMERS_GROUPS_QUERY, true)}
		};
		 
		return loadMainLevel(con, shopName, new DataSelector(CUSTOMERS_KEY_COLUMN, CUSTOMERS_DOC_TYPE, GET_CUSTOMERS_QUERY, false), childSelectors);
	}
}
package com.indigoid.prestashop;

/**
 * Loads product data into memory from the database manager. The products to be
 * loaded are those making the predicate true.
 * 
 * @author Charlie
 *
 */
public class ProductsLoader extends PrstshpLoader {

	/**
	 * document_type JSON property value for this kind of objects
	 */
	private static final String PRODUCTS_DOC_TYPE = "product";
	/**
	 * Key column for the products table
	 */
	private static final String PRODUCTS_KEY_COLUMN = "id_product";
	/**
	 * Key column for the product combinations table (prstshp_product_attribute)
	 */
	private static final String PRODUCT_COMBINATION_KEY_COLUMN = "id_product_attribute";
	/**
	 * JSON property for combination attributes property
	 */
	private static final String COMBINATION_ATTRIBUTES_PROPERTY = "combination_attributes";
	/**
	 * JSON property for product combination property
	 */
	private static final String COMBINATIONS_PROPERTY = "combinations";
	/**
	 * JSON property for product carriers property
	 */
	private static final String CARRIERS_PROPERTY = "carriers";
	/**
	 * JSON property for product features property
	 */
	private static final String FEATURES_PROPERTY = "features";
	/**
	 * JSON property for product tags property
	 */
	private static final String TAGS_PROPERTY = "tags";
	/**
	 * JSON property for customers' comments on products property
	 */
	private static final String COMMENTS_PROPERTY = "comments";
	/**
	 * JSON property for product categories property
	 */
	private static final String CATEGORIES_PROPERTY = "categories";
	/**
	 * JSON property for product descriptions property
	 */
	private static final String DESCRIPTIONS_PROPERTY = "descriptions";
	/**
	 * Select statement to retrieve Products data.
	 */
	private static final String GET_PRODUCTS_QUERY = "SELECT `id_product`, `id_supplier`, `id_manufacturer`, `id_category_default`, "
			+ "`id_shop_default`, `id_tax_rules_group`, `on_sale`, `online_only`, `ean13`, `upc`, `ecotax`, `quantity`, "
			+ "`minimal_quantity`, `price`, `wholesale_price`, `unity`, `unit_price_ratio`, `additional_shipping_cost`, "
			+ "`reference`, `supplier_reference`, `location`, `width`, `height`, `depth`, `weight`, `out_of_stock`, "
			+ "`quantity_discount`, `customizable`, `uploadable_files`, `text_fields`, `active`, `redirect_type`, "
			+ "`id_product_redirected`, `available_for_order`, `available_date`, `condition`, `show_price`, `indexed`, `visibility`, "
			+ "`cache_is_pack`, `cache_has_attachments`, `is_virtual`, `cache_default_attribute`, `date_add`, `date_upd`, "
			+ "`advanced_stock_management`, `pack_stock_type` " + "FROM prstshp_product";
	/**
	 * Statement used to retrieve the description elements that depend on the
	 * language and on the shop.
	 */
	private static final String GET_PRODUCT_DESCRIPTIONS_QUERY = "SELECT `prstshp_product_lang`.`id_shop`, `prstshp_product_lang`.`id_lang`, `prstshp_lang`.`name` as lang, "
			+ "`prstshp_product_lang`.`description`, `prstshp_product_lang`.`description_short`, `prstshp_product_lang`.`link_rewrite`, "
			+ "`prstshp_product_lang`.`meta_description`, `prstshp_product_lang`.`meta_keywords`, `prstshp_product_lang`.`meta_title`, "
			+ "`prstshp_product_lang`.`name`, `prstshp_product_lang`.`available_now`, `prstshp_product_lang`.`available_later`"
			+ "FROM `prstshp_product_lang` "
			+ "INNER JOIN `prstshp_lang` ON `prstshp_product_lang`.`id_lang` = `prstshp_lang`.`id_lang` "
			+ "WHERE `id_product` = ? " + "ORDER BY `prstshp_product_lang`.`id_shop`";
	/**
	 * Statement used to retrieve the carriers associated to a product
	 */
	private static final String GET_PRODUCT_CARRIERS_QUERY = "SELECT `prstshp_product_carrier`.`id_shop`, MAX(`prstshp_carrier`.`id_carrier`) as `id_carrier`, `prstshp_carrier`.`name` "
			+ "FROM `prstshp_product_carrier` "
			+ "INNER JOIN `prstshp_carrier` ON `prstshp_product_carrier`.`id_carrier_reference` = `prstshp_carrier`.`id_reference` "
			+ "WHERE `prstshp_product_carrier`.`id_product` = ? "
			+ "GROUP BY `prstshp_product_carrier`.`id_shop`, `prstshp_product_carrier`.`id_carrier_reference` "
			+ "ORDER BY `prstshp_product_carrier`.`id_shop`, `id_carrier`";
	/**
	 * Statement used to retrieve the comments on a product
	 */
	private static final String GET_PRODUCT_COMMENTS_QUERY = "SELECT `id_product_comment`,  `id_customer`, `id_guest`, `title`, `content`, "
			+ "`customer_name`, `grade`, `validate`, `deleted`, `date_add` " + "FROM `prstshp_product_comment` "
			+ "WHERE `id_product` = ?";
	/**
	 * Statement used to retrieve the tags on a product
	 */
	private static final String GET_PRODUCT_TAGS_QUERY = "SELECT `prstshp_tag`.`id_tag`, `prstshp_tag`.`id_lang`, `prstshp_tag`.`name`,  `prstshp_lang`.`name` as `lang` "
			+ "FROM `prstshp_product_tag` "
			+ "INNER JOIN `prstshp_tag` ON `prstshp_product_tag`.`id_tag` = `prstshp_tag`.`id_tag` "
			+ "INNER JOIN `prstshp_lang` ON `prstshp_product_tag`.`id_lang` = `prstshp_lang`.`id_lang` "
			+ "WHERE `id_product` = ?";
	/**
	 * Statement used to retrieve the features defined on a product
	 */
	private static final String GET_PRODUCT_FEATURES_QUERY = "SELECT `prstshp_feature_product`.`id_feature`, `prstshp_feature_product`.`id_feature_value`, "
			+ "`prstshp_feature_lang`.`id_lang`, `prstshp_feature_lang`.`name`, `prstshp_feature_value_lang`.`value`, `prstshp_lang`.`name` as `lang` "
			+ "FROM `prstshp_feature_product` "
			+ "INNER JOIN `prstshp_feature_lang` ON `prstshp_feature_product`.`id_feature`  = `prstshp_feature_lang`.`id_feature` "
			+ "INNER JOIN `prstshp_feature_value_lang` ON `prstshp_feature_product`.`id_feature_value`  = `prstshp_feature_value_lang`.`id_feature_value` AND `prstshp_feature_lang`.`id_lang`  = `prstshp_feature_value_lang`.`id_lang` "
			+ "INNER JOIN `prstshp_lang` ON `prstshp_feature_lang`.`id_lang` = `prstshp_lang`.`id_lang` "
			+ "WHERE `prstshp_feature_product`.`id_product` = ? " + "ORDER BY `prstshp_feature_product`.`id_feature`";
	/**
	 * Statement used to retrieve the combinations defined on a product
	 */
	private static final String GET_PRODUCT_COMBINATIONS_QUERY = "SELECT t1.id_product_attribute, t1.reference, t1.supplier_reference, t1.location, "
			+ "t1.ean13, t1.upc, t2.id_shop, t2.wholesale_price, t2.price, t2.ecotax, t2.weight, t2.unit_price_impact, t2.default_on, "
			+ "t2.minimal_quantity, t2.available_date " + "FROM  prstshp_product_attribute as t1 "
			+ "JOIN prstshp_product_attribute_shop AS t2 ON t1.id_product = t2.id_product AND t1.id_product_attribute = t2.id_product_attribute "
			+ "WHERE t1.id_product = ?";
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
	 * Statement used to retrieve the categories a product belongs to
	 */
	private static final String GET_PRODUCT_CATEGORIES_QUERY = "SELECT t2.id_shop, t2.id_category, t2.position, t3.id_lang, t4.name as lang, t3.name "
			+ "FROM prstshp_category_product AS t1 "
			+ "JOIN prstshp_category_shop AS t2 ON t1.id_category = t2.id_category "
			+ "JOIN prstshp_category_lang AS t3 ON t1.id_category = t3.id_category AND t2.id_shop = t3.id_shop "
			+ "JOIN prstshp_lang AS t4 ON t4.id_lang = t3.id_lang " + "WHERE t1.id_product = ?";

	/**
	 * Creates a loader for all products.
	 */
	public ProductsLoader() {
		super();
	}

	/**
	 * Creates a loader for products fulfilling a particular predicate
	 * 
	 * @param predicate
	 *            Condition that the selected products must follow.
	 */
	public ProductsLoader(String predicate) {
		super(predicate);
	}

	/**
	 * Load products information from a PrestaShop database.
	 * 
	 * @param con
	 *            The database manager connection
	 * @param shopName
	 *            Name of the shop to identify generated top-level documents
	 * @return The number of products loaded.
	 */
	public int load(com.indigoid.dbutils.MariaDBConnectionManager con, String shopName) {
		// These are the selectors for child elements
		DataSelector[][] childSelectors = {
				{ new DataSelector(PRODUCTS_KEY_COLUMN, DESCRIPTIONS_PROPERTY, GET_PRODUCT_DESCRIPTIONS_QUERY, true) },
				{ new DataSelector(PRODUCTS_KEY_COLUMN, CATEGORIES_PROPERTY, GET_PRODUCT_CATEGORIES_QUERY, true) },
				{ new DataSelector(PRODUCTS_KEY_COLUMN, COMMENTS_PROPERTY, GET_PRODUCT_COMMENTS_QUERY, true) },
				{ new DataSelector(PRODUCTS_KEY_COLUMN, TAGS_PROPERTY, GET_PRODUCT_TAGS_QUERY, true) },
				{ new DataSelector(PRODUCTS_KEY_COLUMN, FEATURES_PROPERTY, GET_PRODUCT_FEATURES_QUERY, true) },
				{ new DataSelector(PRODUCTS_KEY_COLUMN, CARRIERS_PROPERTY, GET_PRODUCT_CARRIERS_QUERY, true) },
				{ new DataSelector(PRODUCTS_KEY_COLUMN, COMBINATIONS_PROPERTY, GET_PRODUCT_COMBINATIONS_QUERY, true), 
				  new DataSelector(PRODUCT_COMBINATION_KEY_COLUMN, COMBINATION_ATTRIBUTES_PROPERTY, GET_PRODUCT_COMBINATION_ATTRIBUTES_QUERY, true) } };

		return loadMainLevel(con, shopName, new DataSelector(PRODUCTS_KEY_COLUMN, PRODUCTS_DOC_TYPE, GET_PRODUCTS_QUERY, false), childSelectors);
	}
}
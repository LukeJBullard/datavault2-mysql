<?php
    /**
     * MySQL addon for DataVault 2.0 Module for OliveWeb
     * 
     * @author Luke Bullard
     */

    //make sure we are included securely
    if (!defined("INPROCESS")) { header("HTTP/1.0 403 Forbidden"); exit(0); }

    /**
     * The MySQL Addon OliveWeb Module
     */
    class MOD_dv2mysql
    {
        protected $m_config;

        /**
         * Retrieves a DV2-MySQL config setting.
         * @param String $a_configItem The name of the config setting to read.
         * @return Any The config setting read.
         * @throws String An error message if the config setting could not be read.
         */
        public function getConfig($a_configItem)
        {
            if (!isset($this->m_config[$a_configItem]))
            {
                throw "Unknown Config Item";
            }

            return $this->m_config[$a_configItem];
        }

        /**
         * Sets a DV2-MySQL config setting.
         * @param String $a_configItem The name of the config setting to set.
         * @param Any $a_value The value to set the config setting to.
         */
        public function setConfig($a_configItem, $a_value)
        {
            $this->m_config[$a_configItem] = $a_value;
        }

        public function __construct()
        {
            //load datavault2 module
            Modules::getInstance()['datavault2'];

            //default config
            $this->setConfig("millisecond_precision", true);

            //load config
            include_once("config.php");

            //load mod src files
            require_once("src/MySQLHubTable.php");
            require_once("src/MySQLLinkTable.php");
            require_once("src/MySQLSatelliteTable.php");
        }
    }
?>
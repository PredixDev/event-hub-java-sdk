package com.ge.predix.eventhub.configuration;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Properties Configurator that allows you to read environment specific properties from a
 * Properties file instead of specifying it in environment.
 *  
 * @author krunal
 *
 */

public class PropertiesConfiguration {

    private Properties props;
    private static final String EVENT_HUB_TEST_PROPERTY_FILE = "event-hub-test.properties";     // property file for local,jenkins test
    private static final String EVENT_HUB_PRODUCTION_PROPERTY_FILE = "event-hub.properties";    // property file for production test
    private static final Logger logger = LoggerFactory.getLogger(PropertiesConfiguration.class);      

    /**
     * Based on the environment(Production,test,stage) , load the properties. 
     * @param environmennt
     * @throws Exception
     */
    public PropertiesConfiguration(Environment environmennt) throws Exception {
        props = new Properties();
      props =  buildProperties(environmennt,null);
    }

    /**
     * Build the properties object using the Environment and path if any specified by api caller. 
     * @param environment  One of the enums
     * @param path  User specified path or defaults to classloader . src/main/resources folder
     * @return  properties object 
     * @throws Exception
     */
    public Properties buildProperties(Environment environment,String path) throws Exception {
       Properties prop;
            if(environment  == null)
                throw new Exception("Type of Environment is required !");

            switch(environment) {
            
                case   PRODUCTION: {
                    
                    prop = buildProperties(EVENT_HUB_PRODUCTION_PROPERTY_FILE, path);
                    break;
                }
                default : { 	
                    prop = buildProperties(EVENT_HUB_TEST_PROPERTY_FILE,path);
                    break;
                }
        }
            return prop;
    }
    
    public Properties getProperties() {
        return this.props;
    }
    
    

    /**
     * Build Property Object
     * @param file  fileName to be used
     * @return   Properties object 
     * @throws Exception
     */
    private Properties buildProperties(String fileName,String path) throws Exception {

        InputStream stream = null;

        try {
            
            ClassLoader classLoader = this.getClass().getClassLoader();
            if(path == null) {
                path = ".";
                logger.info(String.format("Looking for file %s at path %s", fileName,path));
                stream = classLoader.getResourceAsStream(fileName);
            }
            else {
                logger.info(String.format("Looking for file %s at path %s",fileName,path));
                
                if(!path.endsWith("System.getProperty(\"file.separator\")"))
                    path = path+System.getProperty("file.separator")+fileName;
                else
                    path = path+fileName;
                
                stream = classLoader.getResource(fileName).openStream();
            }
            if(stream == null)
                throw new IOException("File  not found at path ");
            props.load(stream);
        } catch (IOException ex) {
            throw new Exception(ex.getMessage());
        } finally{
            if(stream!=null){
                try {
                    stream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        props.forEach((k, v) -> logger.debug(String.format(" key %s : value %s",k ,v)));

        return props;

    }

}

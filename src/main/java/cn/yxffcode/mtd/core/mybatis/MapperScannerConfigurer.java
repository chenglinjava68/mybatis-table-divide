package cn.yxffcode.mtd.core.mybatis;

import cn.yxffcode.mtd.api.DDB;
import com.google.common.base.Throwables;
import org.mybatis.spring.mapper.MapperFactoryBean;
import org.springframework.beans.BeansException;
import org.springframework.beans.PropertyValue;
import org.springframework.beans.PropertyValues;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.AnnotatedBeanDefinition;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.PropertyResourceConfigurer;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.config.TypedStringValue;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ClassPathBeanDefinitionScanner;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.type.classreading.MetadataReader;
import org.springframework.core.type.classreading.MetadataReaderFactory;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.core.type.filter.AssignableTypeFilter;
import org.springframework.core.type.filter.TypeFilter;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.springframework.util.Assert.notNull;

/**
 * We need to inject different {@link org.apache.ibatis.session.SqlSession} for different DAOs.
 * This class is treated to replace the {@link org.mybatis.spring.mapper.MapperScannerConfigurer}
 * due to the mybatis' MapperScannerConfigurer cannot recognize {@link DDB#dbname()}.
 * <p/>
 * An another choice is to write different annotations to specify each database and DAOs marked by
 * different annotations are been loaded by different instance of mybatis' MapperScannerConfigurer.
 * <p/>
 * Writing a special class to separate different databases is more complex but flexible.We can do
 * anything when scan classpath.
 *
 * @author gaohang on 15/12/24.
 */
public class MapperScannerConfigurer implements BeanDefinitionRegistryPostProcessor, InitializingBean, ApplicationContextAware, BeanNameAware {

  private String basePackage;
  private boolean addToConfig = true;
  /**
   * Attention that the sqlSessionFactoryNames cannot be replaced by Map<String, SqlSessionFactory>
   * because of the SqlSessionFactory depends on DataSource, and the DataSource uses
   * properties-placeholder, placeholders are loaded through a BeanFactoryPostProcessor, but this
   * class is an BeanDefinitionRegistryPostProcessor, it's invoked before BeanFactoryPostProcessors.
   * So when DAOs are been scan, placeholders still not be loaded and an exception will be thrown.
   */
  private Map<String, String> sqlSessionFactoryNames;
  private String defaultDbname;
  private Class<?> markerInterface;
  private ApplicationContext applicationContext;
  private String beanName;
  private boolean processPropertyPlaceHolders;

  public void setBasePackage(String basePackage) {
    this.basePackage = basePackage;
  }

  public void setAddToConfig(boolean addToConfig) {
    this.addToConfig = addToConfig;
  }

  public void setMarkerInterface(Class<?> superClass) {
    this.markerInterface = superClass;
  }

  public void setProcessPropertyPlaceHolders(boolean processPropertyPlaceHolders) {
    this.processPropertyPlaceHolders = processPropertyPlaceHolders;
  }

  public void setApplicationContext(ApplicationContext applicationContext) {
    this.applicationContext = applicationContext;
  }

  public void setBeanName(String name) {
    this.beanName = name;
  }

  public void setDefaultDbname(String defaultDbname) {
    this.defaultDbname = defaultDbname;
  }

  public void setSqlSessionFactoryNames(Map<String, String> sqlSessionFactoryNames) {
    this.sqlSessionFactoryNames = sqlSessionFactoryNames;
  }

  public void afterPropertiesSet() throws Exception {
    notNull(this.basePackage, "Property 'basePackage' is required");
  }

  public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) {
  }

  public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry beanDefinitionRegistry)
      throws BeansException {
    if (this.processPropertyPlaceHolders) {
      processPropertyPlaceHolders();
    }

    Scanner scanner = new Scanner(beanDefinitionRegistry);
    scanner.setResourceLoader(this.applicationContext);

    scanner.scan(StringUtils.tokenizeToStringArray(this.basePackage,
        ConfigurableApplicationContext.CONFIG_LOCATION_DELIMITERS));
  }

  private void processPropertyPlaceHolders() {
    Map<String, PropertyResourceConfigurer> prcs =
        applicationContext.getBeansOfType(PropertyResourceConfigurer.class);

    if (!prcs.isEmpty() && applicationContext instanceof GenericApplicationContext) {
      BeanDefinition mapperScannerBean =
          ((GenericApplicationContext) applicationContext).getBeanFactory()
              .getBeanDefinition(beanName);

      DefaultListableBeanFactory factory = new DefaultListableBeanFactory();
      factory.registerBeanDefinition(beanName, mapperScannerBean);

      for (PropertyResourceConfigurer prc : prcs.values()) {
        prc.postProcessBeanFactory(factory);
      }

      PropertyValues values = mapperScannerBean.getPropertyValues();

      this.basePackage = updatePropertyValue("basePackage", values);
    }
  }

  private String updatePropertyValue(String propertyName, PropertyValues values) {
    PropertyValue property = values.getPropertyValue(propertyName);

    if (property == null) {
      return null;
    }

    Object value = property.getValue();

    if (value == null) {
      return null;
    } else if (value instanceof String) {
      return value.toString();
    } else if (value instanceof TypedStringValue) {
      return ((TypedStringValue) value).getValue();
    } else {
      return null;
    }
  }

  private final class Scanner extends ClassPathBeanDefinitionScanner {

    public Scanner(BeanDefinitionRegistry registry) {
      super(registry);
    }

    @Override protected void registerDefaultFilters() {
      addIncludeFilter(new AnnotationTypeFilter(DDB.class));
      if (MapperScannerConfigurer.this.markerInterface != null) {
        addIncludeFilter(new AssignableTypeFilter(MapperScannerConfigurer.this.markerInterface) {
          @Override protected boolean matchClassName(String className) {
            return false;
          }
        });
      }
      addExcludeFilter(new TypeFilter() {
        public boolean match(MetadataReader metadataReader,
                             MetadataReaderFactory metadataReaderFactory) throws IOException {
          String className = metadataReader.getClassMetadata().getClassName();
          return className.endsWith("package-info");
        }
      });
    }

    @Override protected Set<BeanDefinitionHolder> doScan(String... basePackages) {
      Set<BeanDefinitionHolder> beanDefinitions = super.doScan(basePackages);

      if (beanDefinitions.isEmpty()) {
        logger.warn("No MyBatis mapper was found in '" + MapperScannerConfigurer.this.basePackage
            + "' package. Please check your configuration.");
      } else {
        for (BeanDefinitionHolder holder : beanDefinitions) {
          GenericBeanDefinition definition = (GenericBeanDefinition) holder.getBeanDefinition();

          String mappedInterface = definition.getBeanClassName();
          if (logger.isDebugEnabled()) {
            logger.debug("Creating MapperFactoryBean with name '" + holder.getBeanName() + "' and '"
                + mappedInterface + "' mapperInterface");
          }

          definition.getPropertyValues().add("mapperInterface", mappedInterface);
          definition.setBeanClass(MapperFactoryBean.class);

          definition.getPropertyValues()
              .add("addToConfig", MapperScannerConfigurer.this.addToConfig);

          try {
            Class<?> type =
                ClassUtils.forName(mappedInterface, Thread.currentThread().getContextClassLoader());
            DDB annotation = type.getAnnotation(DDB.class);
            String dbname = annotation.dbname();
            if (isBlank(dbname)) {
              dbname = defaultDbname;
            }
            checkState(isNotBlank(dbname));
            String sqlSessionFactoryName =
                MapperScannerConfigurer.this.sqlSessionFactoryNames.get(dbname);
            checkNotNull(sqlSessionFactoryName, "no such SqlSessionFactory:%s", dbname);
            definition.getPropertyValues()
                .add("sqlSessionFactory", new RuntimeBeanReference(sqlSessionFactoryName));
          } catch (ClassNotFoundException e) {
            Throwables.propagate(e);
          }
        }
      }

      return beanDefinitions;
    }

    @Override protected boolean isCandidateComponent(AnnotatedBeanDefinition beanDefinition) {
      return (beanDefinition.getMetadata().isInterface() && beanDefinition.getMetadata()
          .isIndependent());
    }

    @Override protected boolean checkCandidate(String beanName, BeanDefinition beanDefinition)
        throws IllegalStateException {
      if (super.checkCandidate(beanName, beanDefinition)) {
        return true;
      } else {
        logger.warn("Skipping MapperFactoryBean with name '" + beanName + "' and '" + beanDefinition
            .getBeanClassName() + "' mapperInterface"
            + ". Bean already defined with the same name!");
        return false;
      }
    }
  }

}

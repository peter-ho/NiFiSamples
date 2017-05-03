package com.teradata.processors.yarn;

import java.io.File;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.hadoop.KerberosProperties;
import org.apache.nifi.hadoop.SecurityUtil;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

@Tags({ "yarn", "mapreduce" })
@CapabilityDescription("Execute a yarn job")
@DynamicProperty(name = "Hadoop configuration key", value = "Hadoop configuration value", supportsExpressionLanguage = true, description = "Conifguration to be sent to YARN for job execution")
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = ExecuteYARNMapReduceJob.ATTR_YARN_JOB_ID, description = "Id of the yarn job submitted.")
 })
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class ExecuteYARNMapReduceJob extends AbstractProcessor {

	protected Set<Relationship> relationships;
	protected List<PropertyDescriptor> descriptors;
	protected List<String> configKeysExpression = new ArrayList<String>();
	private Map<String, String> configValues = new HashMap<String, String>();
	protected Map<String, String> configValuesReadOnly;

	public static final PropertyDescriptor HADOOP_CONFIGURATION_RESOURCES = new PropertyDescriptor.Builder()
			.name("Hadoop Configuration Resources")
			.description(
					"A comma separated list of files which contains the Hadoop file system configuration within core-site.xml, mapreduce-site.xml, yarn-site.xml, and hdfs-site.xml. Without this, Hadoop "
							+ "will search the classpath for a 'core-site.xml', 'mapreduce-site.xml', 'yarn-site.xml', 'hdfs-site.xml' file or will revert to a default configuration.")
			.required(true)
			.addValidator(StandardValidators.NON_BLANK_VALIDATOR).build();
	public static final PropertyDescriptor JOB_JAR_PATH = new PropertyDescriptor.Builder()
			.name("Job JAR Path").description("Local path of the jar with classes requried for the job")
			.required(true)
			.addValidator(StandardValidators.NON_BLANK_VALIDATOR)
			.sensitive(false).expressionLanguageSupported(true).build();
	public static final PropertyDescriptor JOB_NAME = new PropertyDescriptor.Builder()
			.name("Job Name").description("Name of the YARN job")
			.defaultValue(ExecuteYARNMapReduceJob.class.getSimpleName()).required(false)
			.addValidator(StandardValidators.NON_BLANK_VALIDATOR)
			.sensitive(false).expressionLanguageSupported(true).build();
	public static final PropertyDescriptor JOB_QUEUE = new PropertyDescriptor.Builder()
			.name("YARN Queue").description("Name of the yarn queue for the job to be submitted to")
			.required(false).addValidator(StandardValidators.NON_BLANK_VALIDATOR)
			.defaultValue("default")
			.sensitive(false).expressionLanguageSupported(true).build();
	public static final PropertyDescriptor INPUT_PATHS = new PropertyDescriptor.Builder()
			.name("Input Paths")
			.description("Comma delimited HDFS path with input data")
			.required(false)
			.addValidator(StandardValidators.NON_BLANK_VALIDATOR)
			.sensitive(false).expressionLanguageSupported(true).build();
	public static final PropertyDescriptor OUTPUT_PATH = new PropertyDescriptor.Builder()
			.name("Output Path")
			.description("HDFS path for output to be written to")
			.required(false)
			.addValidator(StandardValidators.NON_BLANK_VALIDATOR)
			.sensitive(false).expressionLanguageSupported(true).build();
	public static final PropertyDescriptor MAPPER_CLASS = new PropertyDescriptor.Builder()
			.name("Mapper Class")
			.description("Full class name including package name for mapper")
			.required(true)
			.addValidator(StandardValidators.NON_BLANK_VALIDATOR)
			.sensitive(false).expressionLanguageSupported(true).build();
	public static final PropertyDescriptor REDUCER_CLASS = new PropertyDescriptor.Builder()
			.name("Reducer Class")
			.description("Full class name with package name for reducer")
			.required(false)
			.addValidator(StandardValidators.NON_BLANK_VALIDATOR)
			.sensitive(false).expressionLanguageSupported(true).build();
	public static final PropertyDescriptor REDUCER_NUM = new PropertyDescriptor.Builder()
			.name("Number of Reducer").description("Number of reducer")
			.required(false).defaultValue("1")
			.addValidator(StandardValidators.INTEGER_VALIDATOR)
			.sensitive(false).expressionLanguageSupported(true).build();
	public static final PropertyDescriptor COMBINER_CLASS = new PropertyDescriptor.Builder()
			.name("Combiner Class")
			.description("Full class name with package name for combiner")
			.required(false)
			.addValidator(StandardValidators.NON_BLANK_VALIDATOR)
			.sensitive(false).expressionLanguageSupported(true).build();

	public static final PropertyDescriptor INPUT_FORMAT_CLASS = new PropertyDescriptor.Builder()
			.name("Input Format Class")
			.description("Full class name of the input format").required(false)
			.addValidator(StandardValidators.NON_BLANK_VALIDATOR)
			.sensitive(false).expressionLanguageSupported(true).build();
	public static final PropertyDescriptor OUTPUT_KEY_CLASS = new PropertyDescriptor.Builder()
			.name("Output Key Class")
			.description("Full class name of the output key").required(true)
			.addValidator(StandardValidators.NON_BLANK_VALIDATOR)
			.sensitive(false).expressionLanguageSupported(true).build();
	public static final PropertyDescriptor OUTPUT_VALUE_CLASS = new PropertyDescriptor.Builder()
			.name("Output Value Class")
			.description("Full class name of the output value").required(true)
			.addValidator(StandardValidators.NON_BLANK_VALIDATOR)
			.sensitive(false).expressionLanguageSupported(true).build();

	public static final PropertyDescriptor CLASS_PATH_ARCHIVE = new PropertyDescriptor.Builder()
			.name("Class Path Archive")
			.description("Path of an archive to be added to the class path")
			.required(false)
			.addValidator(StandardValidators.NON_BLANK_VALIDATOR)
			.sensitive(false).expressionLanguageSupported(true).build();
	public static final PropertyDescriptor CACHED_FILE_URLS = new PropertyDescriptor.Builder()
			.name("Cache file URLs")
			.description(
					"Comma delimited path of files to be added to Hadoop distributed cache")
			.required(false)
			.addValidator(StandardValidators.NON_BLANK_VALIDATOR)
			.sensitive(false).expressionLanguageSupported(true).build();
	public static final PropertyDescriptor CACHED_ARCHIVES_URLS = new PropertyDescriptor.Builder()
			.name("Cache archive file URLs")
			.description("Comma delimited path of archives to be added to Hadoop distributed cache")
			.required(false)
			.addValidator(StandardValidators.NON_BLANK_VALIDATOR)
			.sensitive(false).expressionLanguageSupported(true).build();

	public static final Relationship REL_SUCCESS = new Relationship.Builder()
			.name("success").description("YARN mapreduce ran successfully.")
			.build();
	public static final Relationship REL_FAILURE = new Relationship.Builder()
			.name("failure").description("YARN mapreduce job failed.").build();

	public static final String ATTR_YARN_JOB_ID = "yarn.job.id";
	
	@Override
	protected void init(final ProcessorInitializationContext context) {
		initYARNResources();

		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		addConnectionProperties(descriptors);
		addKerberosProperties(descriptors, context);
		addJobProperties(descriptors);
		this.descriptors = descriptors;

		final Set<Relationship> relationships = new HashSet<Relationship>();
		relationships.add(REL_SUCCESS);
		relationships.add(REL_FAILURE);
		this.relationships = relationships;
	}

	protected void initYARNResources() {
		yarnResources.set(new YARNResources(null, null));
	}
	
	protected void addConnectionProperties(List<PropertyDescriptor> descs) {
		descs.add(HADOOP_CONFIGURATION_RESOURCES);
	}

	protected void addKerberosProperties(List<PropertyDescriptor> descs,
			ProcessorInitializationContext context) {
		kerberosConfigFile = context.getKerberosConfigurationFile();
		kerberosProperties = getKerberosProperties(kerberosConfigFile);
		descs.add(kerberosProperties.getKerberosPrincipal());
		descs.add(kerberosProperties.getKerberosKeytab());
		descs.add(KERBEROS_RELOGIN_PERIOD);
	}

	protected void addJobProperties(List<PropertyDescriptor> descs) {
		descs.add(JOB_JAR_PATH);
		descs.add(JOB_NAME);
		descs.add(JOB_QUEUE);
		descs.add(INPUT_PATHS);
		descs.add(OUTPUT_PATH);
		descs.add(COMBINER_CLASS);
		descs.add(MAPPER_CLASS);
		descs.add(REDUCER_CLASS);
		descs.add(REDUCER_NUM);
		descs.add(OUTPUT_KEY_CLASS);
		descs.add(OUTPUT_VALUE_CLASS);
		descs.add(CLASS_PATH_ARCHIVE);
		descs.add(CACHED_FILE_URLS);
		descs.add(CACHED_ARCHIVES_URLS);
	}

	@Override
	public Set<Relationship> getRelationships() {
		return Collections.unmodifiableSet(this.relationships);
	}

	@Override
	protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return Collections.unmodifiableList(this.descriptors);
	}

	@Override
	public void onTrigger(ProcessContext context, ProcessSession session)
			throws ProcessException {

		final FlowFile flowFile = session.get();
		if (flowFile == null)
			return;

		FlowFile targetFlowFile = flowFile;
		final ComponentLog logger = getLogger();
		final String jobName = loadProperty(context, JOB_NAME, flowFile);
		final String jobJarPath = loadProperty(context, JOB_JAR_PATH, flowFile);
		final String jobQueue = loadProperty(context, JOB_QUEUE, flowFile);
		final String mapperClsName = loadProperty(context, MAPPER_CLASS, flowFile);
		final String reducerClsName = loadProperty(context, REDUCER_CLASS, flowFile);
		final String combinerClsName = loadProperty(context, COMBINER_CLASS, flowFile);
		final String inputFormatClsName = loadProperty(context, INPUT_FORMAT_CLASS, flowFile);
		final String outputKeyClsName = loadProperty(context, OUTPUT_KEY_CLASS, flowFile);
		final String outputValueClsName = loadProperty(context, OUTPUT_VALUE_CLASS, flowFile);
		final Integer reducerNum = loadPropertyAsInteger(context, REDUCER_NUM, flowFile);
		final String inputPaths = loadProperty(context, INPUT_PATHS, flowFile);
		final String outputPath = loadProperty(context, OUTPUT_PATH, flowFile);

		final String cachedFileUrls = loadProperty(context, CACHED_FILE_URLS, flowFile); // MRJobConfig.CACHE_FILES
		final String cachedArchivesUrls = loadProperty(context, CACHED_ARCHIVES_URLS, flowFile); // MRJobConfig.CACHE_ARCHIVES
		final String classPathArchive = loadProperty(context, CLASS_PATH_ARCHIVE, flowFile); // MRJobConfig.CLASSPATH_ARCHIVES

		Configuration config = getConfiguration();
		logger.info(String.format("Loaded Hadoop Config: %s", config.toString()));

		loadDynamicProperties(context, flowFile, context.getProperties(), config);

		Job job;
		try {
			if (!StringUtils.isBlank(jobQueue)) 
				config.set("mapreduce.job.queuename", jobQueue);
			if (!StringUtils.isBlank(mapperClsName))
				config.set(MRJobConfig.MAP_CLASS_ATTR, mapperClsName);
			if (!StringUtils.isBlank(reducerClsName))
				config.set(MRJobConfig.REDUCE_CLASS_ATTR, reducerClsName);
			if (!StringUtils.isBlank(outputKeyClsName))
				config.set(JobContext.OUTPUT_KEY_CLASS, outputKeyClsName);
			if (!StringUtils.isBlank(outputValueClsName))
				config.set(JobContext.OUTPUT_VALUE_CLASS, outputValueClsName);
			if (!StringUtils.isBlank(combinerClsName))
				config.set(JobContext.COMBINE_CLASS_ATTR, combinerClsName);
			if (!StringUtils.isBlank(inputFormatClsName))
				config.set(JobContext.INPUT_FORMAT_CLASS_ATTR,
						inputFormatClsName);
			job = getYARNJobAsUser(config, jobName);
			if (!StringUtils.isBlank(cachedFileUrls))
				job.setCacheFiles(org.apache.hadoop.util.StringUtils
						.stringToURI(StringUtils.split(cachedFileUrls, ','))); // MRJobConfig.CACHE_FILES
			if (!StringUtils.isBlank(cachedArchivesUrls))
				job.setCacheArchives(org.apache.hadoop.util.StringUtils
						.stringToURI(StringUtils.split(cachedArchivesUrls))); // MRJobConfig.CACHE_ARCHIVES
			if (!StringUtils.isBlank(classPathArchive))
				job.addArchiveToClassPath(new Path(classPathArchive)); // MRJobConfig.CLASSPATH_ARCHIVES
			if (reducerNum != null) job.setNumReduceTasks(reducerNum);
			if (jobJarPath != null) job.setJar(jobJarPath);
			if (inputPaths != null) {
				for (String path : StringUtils.split(inputPaths, ',')) {
					FileInputFormat.addInputPath(job, new Path(path));
				}
			}
			if (outputPath != null) FileOutputFormat.setOutputPath(job, new Path(outputPath));
			job.submit();
			String yarnAppId = String.format("application_%1s_%2$04d", job.getStatus().getJobID().getJtIdentifier(), job.getStatus().getJobID().getId());
			targetFlowFile = session.putAttribute(targetFlowFile, ATTR_YARN_JOB_ID, yarnAppId);
			if (job.waitForCompletion(true)) {
				targetFlowFile = parseSuccessJobOutput(context, session, job, targetFlowFile, logger, yarnAppId);
			} else {
				targetFlowFile = parseFailureJobOutput(context, session, job, targetFlowFile, logger, yarnAppId);
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new ProcessException("YARN job creation failed.", e);
		} catch (ClassNotFoundException | InterruptedException e) {
			e.printStackTrace();
			throw new ProcessException("YARN job submit failed.", e);
		}
	}
	
	protected FlowFile parseSuccessJobOutput(ProcessContext context, ProcessSession session, Job job, FlowFile file, ComponentLog logger, String yarnAppId) throws IOException, InterruptedException {
		long jobTime = job.getFinishTime() - job.getStartTime();
		session.getProvenanceReporter().fetch(file, yarnAppId, jobTime);
		session.transfer(file, REL_SUCCESS);
		return file;
	}
	
	protected FlowFile parseFailureJobOutput(ProcessContext context, ProcessSession session, Job job, FlowFile file, ComponentLog logger, String yarnAppId) throws IOException, InterruptedException {
		for (TaskReport tr : job.getTaskReports(TaskType.MAP)) {
			logger.error(StringUtils.join(tr.getDiagnostics(), "\n"));
		}
		session.transfer(session.penalize(file), REL_FAILURE);
        session.getProvenanceReporter().route(file, REL_FAILURE);
		return file;
	}
	
	public static String loadProperty(final ProcessContext context, PropertyDescriptor propDesc, FlowFile flowFile) {
		PropertyValue val = context.getProperty(propDesc);
		if (val == null) return null;
		return (propDesc.isExpressionLanguageSupported())? val.evaluateAttributeExpressions(flowFile).getValue()
				: val.getValue();
	}

	public static Integer loadPropertyAsInteger(final ProcessContext context, PropertyDescriptor propDesc, FlowFile flowFile) {
		PropertyValue val = context.getProperty(propDesc);
		if (val == null) return null;
		return (propDesc.isExpressionLanguageSupported())? val.evaluateAttributeExpressions(flowFile).asInteger()
				: val.asInteger();
	}

	public static Boolean loadPropertyAsBoolean(final ProcessContext context, PropertyDescriptor propDesc, FlowFile flowFile) {
		PropertyValue val = context.getProperty(propDesc);
		if (val == null) return null;
		return (propDesc.isExpressionLanguageSupported())? val.evaluateAttributeExpressions(flowFile).asBoolean()
				: val.asBoolean();
	}

	protected void loadDynamicProperties(final ProcessContext context,
			final FlowFile flowFile,
			final Map<PropertyDescriptor, String> properties,
			Configuration config) {
		for (final Map.Entry<PropertyDescriptor, String> entry : properties
				.entrySet()) {
			String propName = entry.getKey().getName();
			if (propName.indexOf(' ') < 0) {
				String val = context.getProperty(entry.getKey()).evaluateAttributeExpressions(flowFile).getValue();
				if (val != null)
					config.set(propName, context.getProperty(entry.getKey())
							.evaluateAttributeExpressions(flowFile).getValue());
			}
		}
	}

	public static Configuration getConfigurationFromResources(
			String configResources) throws IOException {
		boolean foundResources = false;
		final Configuration config = new ExtendedConfiguration();
		if (null != configResources) {
			String[] resources = configResources.split(",");
			for (String resource : resources) {
				config.addResource(new Path(resource.trim()));
				foundResources = true;
			}
		}

		if (!foundResources) {
			// check that at least 1 non-default resource is available on the
			// classpath
			String configStr = config.toString();
			for (String resource : configStr.substring(
					configStr.indexOf(":") + 1).split(",")) {
				if (!resource.contains("default")
						&& config.getResource(resource.trim()) != null) {
					foundResources = true;
					break;
				}
			}
		}

		if (!foundResources) {
			throw new IOException("Could not find any of the "
					+ HADOOP_CONFIGURATION_RESOURCES.getName()
					+ " on the classpath");
		}
		return config;
	}

	@Override
	protected Collection<ValidationResult> customValidate(
			ValidationContext validationContext) {
		final String configResources = validationContext.getProperty(
				HADOOP_CONFIGURATION_RESOURCES).getValue();
		final String principal = validationContext.getProperty(
				kerberosProperties.getKerberosPrincipal()).getValue();
		final String keytab = validationContext.getProperty(
				kerberosProperties.getKerberosKeytab()).getValue();

		final List<ValidationResult> results = new ArrayList<>();

		if (!StringUtils.isBlank(configResources)) {
			try {
				ValidationResources resources = validationResourceHolder.get();

				// if no resources in the holder, or if the holder has different
				// resources loaded,
				// then load the Configuration and set the new resources in the
				// holder
				if (resources == null
						|| !configResources.equals(resources
								.getConfigResources())) {
					getLogger().debug("Reloading validation resources");
					resources = new ValidationResources(configResources,
							getConfigurationFromResources(configResources));
					validationResourceHolder.set(resources);
				}

				final Configuration conf = resources.getConfiguration();
				results.addAll(KerberosProperties.validatePrincipalAndKeytab(
						this.getClass().getSimpleName(), conf, principal,
						keytab, getLogger()));

			} catch (IOException e) {
				results.add(new ValidationResult.Builder()
						.valid(false)
						.subject(this.getClass().getSimpleName())
						.explanation(
								"Could not load Hadoop Configuration resources")
						.build());
			}
		}

		return results;
	}

	static protected class ValidationResources {
		private final String configResources;
		private final Configuration configuration;

		public ValidationResources(String configResources,
				Configuration configuration) {
			this.configResources = configResources;
			this.configuration = configuration;
		}

		public String getConfigResources() {
			return configResources;
		}

		public Configuration getConfiguration() {
			return configuration;
		}
	}

	/**
	 * Extending Hadoop Configuration to prevent it from caching classes that
	 * can't be found. Since users may be adding additional JARs to the
	 * classpath we don't want them to have to restart the JVM to be able to
	 * load something that was previously not found, but might now be available.
	 *
	 * Reference the original getClassByNameOrNull from Configuration.
	 */
	static class ExtendedConfiguration extends Configuration {

		private final Map<ClassLoader, Map<String, WeakReference<Class<?>>>> CACHE_CLASSES = new WeakHashMap<>();

		public Class<?> getClassByNameOrNull(String name) {
			Map<String, WeakReference<Class<?>>> map;

			synchronized (CACHE_CLASSES) {
				map = CACHE_CLASSES.get(getClassLoader());
				if (map == null) {
					map = Collections.synchronizedMap(new WeakHashMap<>());
					CACHE_CLASSES.put(getClassLoader(), map);
				}
			}

			Class<?> clazz = null;
			WeakReference<Class<?>> ref = map.get(name);
			if (ref != null) {
				clazz = ref.get();
			}

			if (clazz == null) {
				try {
					clazz = Class.forName(name, true, getClassLoader());
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
					return null;
				}
				// two putters can race here, but they'll put the same class
				map.put(name, new WeakReference<>(clazz));
				return clazz;
			} else {
				// cache hit
				return clazz;
			}
		}

	}

	public static final PropertyDescriptor KERBEROS_RELOGIN_PERIOD = new PropertyDescriptor.Builder()
			.name("Kerberos Relogin Period")
			.required(false)
			.description(
					"Period of time which should pass before attempting a kerberos relogin")
			.defaultValue("4 hours")
			.addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	private static final Object RESOURCES_LOCK = new Object();

	private long kerberosReloginThreshold;
	private long lastKerberosReloginTime;
	protected KerberosProperties kerberosProperties;
	private volatile File kerberosConfigFile = null;

	// variables shared by all threads of this processor
	// Hadoop Configuration, and UserGroupInformation (optional)
	private final AtomicReference<YARNResources> yarnResources = new AtomicReference<>();

	// Holder of cached Configuration information so validation does not reload
	// the same config over and over
	private final AtomicReference<ValidationResources> validationResourceHolder = new AtomicReference<>();

	protected static KerberosProperties getKerberosProperties(File kerberosConfigFile) {
		return new KerberosProperties(kerberosConfigFile);
	}

	/*
	 * If your subclass also has an @OnScheduled annotated method and you need
	 * yarnResources in that method, then be sure to call
	 * super.abstractOnScheduled(context)
	 */
	@OnScheduled
	public final void abstractOnScheduled(ProcessContext context)
			throws IOException {
		try {
			// This value will be null when called from ListHDFS, because it
			// overrides all of the default
			// properties this processor sets. TODO: re-work ListHDFS to utilize
			// Kerberos
			if (context.getProperty(KERBEROS_RELOGIN_PERIOD).getValue() != null) {
				kerberosReloginThreshold = context.getProperty(
						KERBEROS_RELOGIN_PERIOD).asTimePeriod(TimeUnit.SECONDS);
			}
			YARNResources resources = yarnResources.get();
			if (resources.getConfiguration() == null) {
				final String configResources = context.getProperty(
						HADOOP_CONFIGURATION_RESOURCES).getValue();
				resources = resetYARNResources(configResources, context);
				yarnResources.set(resources);
			}
		} catch (IOException ex) {
			getLogger().error("HDFS Configuration error - {}",
					new Object[] { ex });
			initYARNResources();
			throw ex;
		}
		
		for (String configKey : configKeysExpression) {
			configValues.put(configKey, context.newPropertyValue(String.format("${%s}", configKey)).evaluateAttributeExpressions().getValue());
		}
		configValuesReadOnly = Collections.unmodifiableMap(configValues);
	}

	@OnStopped
	public final void abstractOnStopped() {
		initYARNResources();
	}

	/*
	 * Reset Hadoop Configuration based on the supplied configuration resources.
	 */
	protected YARNResources resetYARNResources(String configResources,
			ProcessContext context) throws IOException {
		Configuration config = getConfigurationFromResources(configResources);
		config.setClassLoader(Thread.currentThread().getContextClassLoader()); // set
																				// the
																				// InstanceClassLoader

		// disable caching of Configuration objects, else we
		// cannot reconfigure the processor without a complete
		// restart
		String disableCacheName = String.format("fs.%s.impl.disable.cache",
				FileSystem.getDefaultUri(config).getScheme());
		config.set(disableCacheName, "true");

		UserGroupInformation ugi;
		synchronized (RESOURCES_LOCK) {
			if (SecurityUtil.isSecurityEnabled(config)) {
				String principal = context.getProperty(
						kerberosProperties.getKerberosPrincipal()).getValue();
				String keyTab = context.getProperty(
						kerberosProperties.getKerberosKeytab()).getValue();
				ugi = SecurityUtil.loginKerberos(config, principal, keyTab);
				lastKerberosReloginTime = System.currentTimeMillis() / 1000;
			} else {
				config.set("ipc.client.fallback-to-simple-auth-allowed", "true");
				config.set("hadoop.security.authentication", "simple");
				ugi = SecurityUtil.loginSimple(config);
			}
		}

		return new YARNResources(config, ugi);
	}

	protected Job getYARNJobAsUser(final Configuration config, String jobName) throws IOException {
        UserGroupInformation userGroupInformation = yarnResources.get().getUserGroupInformation();
        if (userGroupInformation != null && isTicketOld()) {
            tryKerberosRelogin(userGroupInformation);
        }
		return getYARNJobAsUser(config, jobName, userGroupInformation);
	}
	
	private Job getYARNJobAsUser(final Configuration config, String jobName, UserGroupInformation ugi) throws IOException {
        try {
            return ugi.doAs(new PrivilegedExceptionAction<Job>() {
                @Override
                public Job run() throws Exception {
                    return Job.getInstance(config, jobName);
                }
            });
        } catch (InterruptedException e) {
            throw new IOException("Unable to create yarn job: " + e.getMessage());
        }
    }

	@Override
	protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(
			final String propertyDescriptorName) {
		return new PropertyDescriptor.Builder()
				.name(propertyDescriptorName)
				.required(false)
				.addValidator(
						StandardValidators
								.createAttributeExpressionLanguageValidator(
										org.apache.nifi.expression.AttributeExpression.ResultType.STRING,
										true))
				.addValidator(
						StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
				.addValidator(new Validator() {
					@Override
					public ValidationResult validate(final String subject,
							final String input, final ValidationContext context) {
						if (context.isExpressionLanguageSupported(subject)
								&& context.isExpressionLanguagePresent(input)) {
							return new ValidationResult.Builder()
									.subject(subject).input(input)
									.explanation("Expression Language Present")
									.valid(true).build();
						}

						final boolean isValid = !(StringUtils.isBlank(input) || StringUtils
								.contains(input, ' '));
						return new ValidationResult.Builder()
								.input(input)
								.subject(subject)
								.valid(isValid)
								.explanation(
										isValid ? null
												: "Hadoop configuration properties cannot contain space(s)")
								.build();
					}
				}).expressionLanguageSupported(true).dynamic(true).build();
	}

	protected Configuration getConfiguration() {
		return yarnResources.get().getConfiguration();
	}

	protected void tryKerberosRelogin(UserGroupInformation ugi) {
		try {
			getLogger()
					.info("Kerberos ticket age exceeds threshold [{} seconds] "
							+ "attempting to renew ticket for user {}",
							new Object[] { kerberosReloginThreshold,
									ugi.getUserName() });
			ugi.checkTGTAndReloginFromKeytab();
			lastKerberosReloginTime = System.currentTimeMillis() / 1000;
			getLogger().info(
					"Kerberos relogin successful or ticket still valid");
		} catch (IOException e) {
			// Most likely case of this happening is ticket is expired and error
			// getting a new one,
			// meaning dfs operations would fail
			getLogger().error("Kerberos relogin failed", e);
			throw new ProcessException("Unable to renew kerberos ticket", e);
		}
	}

	protected boolean isTicketOld() {
		return (System.currentTimeMillis() / 1000 - lastKerberosReloginTime) > kerberosReloginThreshold;
	}

	public static class YARNResources {
		private final Configuration configuration;
		private final UserGroupInformation userGroupInformation;

		public YARNResources(Configuration configuration,
				UserGroupInformation userGroupInformation) {
			this.configuration = configuration;
			this.userGroupInformation = userGroupInformation;
		}

		public Configuration getConfiguration() {
			return configuration;
		}

		public UserGroupInformation getUserGroupInformation() {
			return userGroupInformation;
		}
	}
}

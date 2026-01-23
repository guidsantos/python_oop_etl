from .logger_setup import LoggerSetup

logger = LoggerSetup().get_logger()


class GlueSetup:
    """
    Manages AWS Glue Context and Job initialization.
    """
    def __init__(self, spark_session, args):
        self.sc = spark_session.sparkContext
        self.glue_context = None
        self.job = None
        self.args = args
        self._init_glue()

    def _init_glue(self):
        try:
            from awsglue.context import GlueContext
            from awsglue.job import Job
            
            self.glue_context = GlueContext(self.sc)
            self.job = Job(self.glue_context)
            
            job_name = self.args.get('JOB_NAME', 'local_test_job')
            self.job.init(job_name, self.args.args)
            logger.info("Glue Context and Job initialized.")
        except ImportError:
            logger.warning("AWS Glue modules not found. Running in local/Spark-only mode.")
            self.glue_context = None
            self.job = None

    def get_context(self):
        return self.glue_context

    def commit(self):
        if self.job:
            self.job.commit()
            logger.info("Glue Job committed.")

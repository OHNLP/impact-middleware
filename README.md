# IMPACT Middleware
Supports retention of phenotype definitions and query/adjudication results, textual -> UMLS phenotype definition resolution, and job execution/management functionality

Documentation on REST endpoints can be found at [https://www.github.com/OHNLP/IMPACT-documentation](https://www.github.com/OHNLP/IMPACT-documentation)

You will require Java 9+ and maven. 
To compile, execute git clone https://github.com/OHNLP/impact-backend.git and execute `mvn clean install` in the cloned directory.
Additionally, you will want to copy [the sample application configuration](https://github.com/OHNLP/impact-middleware/blob/master/src/main/resources/application-template.yml) and place the contents within an `application.yml` file in the same directory as the compiled jar after editing your configuration settings appropriately.
Of note, Apache Flink is the recommended runner and is what is used in the sample configuration. 

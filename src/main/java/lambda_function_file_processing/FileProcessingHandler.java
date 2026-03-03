package lambda_function_file_processing;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.pdfbox.Loader;
import org.apache.pdfbox.io.RandomAccessReadBuffer;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.encryption.AccessPermission;
import org.apache.pdfbox.pdmodel.encryption.StandardProtectionPolicy;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class FileProcessingHandler implements RequestHandler<S3Event,String> {
	
	private static final S3Client S3CLIENT = S3Client.builder()
			 .endpointOverride(URI.create("http://localstack:4566"))
			 .region(Region.AP_SOUTH_1)
			 .credentialsProvider(
					 StaticCredentialsProvider.create(
							 AwsBasicCredentials.create("test","test")
					 )
			  ).serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
			 .build();
	
//	@Override
//	public String handleRequest(S3Event event, Context context) {
//
//	    context.getLogger().log("🔥 HANDLER ENTERED 🔥\n");
//
//	    try {
//	        context.getLogger().log("Records size: " +
//	                (event.getRecords() == null ? "null" : event.getRecords().size()) + "\n");
//	    } catch (Exception e) {
//	        context.getLogger().log("Exception in test: " + e.getMessage() + "\n");
//	        throw new RuntimeException(e);
//	    }
//
//	    return "ok";
//	}
	
//	@Override
//	public String handleRequest(S3Event event, Context context) {
//	    System.out.println("🔥 BADRI VERSION 2 EXECUTED 🔥");
//	    return "debug";
//	}
	
	@Override
	public String handleRequest(S3Event event, Context context) {

	    context.getLogger().log("=== LAMBDA STARTED ===\n");

	    try {

	        if (event == null) {
	            context.getLogger().log("Event is NULL\n");
	            return "Event null";
	        }

	        if (event.getRecords() == null) {
	            context.getLogger().log("Records are NULL\n");
	            return "Records null";
	        }

	        context.getLogger().log("Records size: " + event.getRecords().size() + "\n");

	        for (S3EventNotification.S3EventNotificationRecord record : event.getRecords()) {

	            String sourceBucket = record.getS3().getBucket().getName();

	            String objKey = java.net.URLDecoder.decode(
	                    record.getS3().getObject().getKey(),
	                    java.nio.charset.StandardCharsets.UTF_8
	            );

	            context.getLogger().log("Source Bucket: " + sourceBucket + "\n");
	            context.getLogger().log("Object Key: " + objKey + "\n");

	            // Download from source bucket
	            GetObjectRequest getObjReq = GetObjectRequest.builder()
	                    .bucket(sourceBucket)
	                    .key(objKey)
	                    .build();

	            context.getLogger().log("Downloading file from source bucket...\n");

	            ResponseInputStream<GetObjectResponse> inputStream =
	                    S3CLIENT.getObject(getObjReq);

	            context.getLogger().log("File downloaded successfully\n");

	            // Encrypt PDF
	            byte[] protectedPdf = encryptPdf(inputStream, "Badri@02");

	            context.getLogger().log("PDF encrypted successfully\n");

	            ByteArrayInputStream bis = new ByteArrayInputStream(protectedPdf);

	            // Upload to destination bucket
	            PutObjectRequest putObjReq = PutObjectRequest.builder()
	                    .bucket("dest-bucket")
	                    .key(objKey)
	                    .contentType("application/pdf")
	                    .build();

	            context.getLogger().log("Uploading to destination bucket...\n");

	            S3CLIENT.putObject(
	                    putObjReq,
	                    RequestBody.fromInputStream(bis, protectedPdf.length)
	            );

	            context.getLogger().log("File uploaded to dest-bucket successfully\n");
	        }

	    } catch (Exception e) {

	        context.getLogger().log("=== ERROR OCCURRED ===\n");
	        context.getLogger().log("Error Message: " + e.getMessage() + "\n");

	        java.io.StringWriter sw = new java.io.StringWriter();
	        java.io.PrintWriter pw = new java.io.PrintWriter(sw);
	        e.printStackTrace(pw);

	        context.getLogger().log(sw.toString());

	        throw new RuntimeException(e);
	    }

	    context.getLogger().log("=== LAMBDA FINISHED SUCCESSFULLY ===\n");
	    return "success";
	}
	
	public byte[] encryptPdf(InputStream inputStream,String password) throws IOException {
		PDDocument document = Loader.loadPDF(new RandomAccessReadBuffer(inputStream));
		
		AccessPermission ap = new AccessPermission();
		
		StandardProtectionPolicy spp = new StandardProtectionPolicy("owner123", password, ap);
		
		spp.setEncryptionKeyLength(128);
		spp.setPermissions(ap);
		
		document.protect(spp);
		
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		
		document.save(bos);
		
		document.close();
		
		return bos.toByteArray();
	}
}

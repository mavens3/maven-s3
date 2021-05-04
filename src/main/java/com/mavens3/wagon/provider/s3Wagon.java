package com.mavens3.wagon.provider;

import java.io.File;
import java.util.Date;

import org.apache.maven.wagon.AbstractWagon;
import org.apache.maven.wagon.ConnectionException;
import org.apache.maven.wagon.ResourceDoesNotExistException;
import org.apache.maven.wagon.TransferFailedException;
import org.apache.maven.wagon.authentication.AuthenticationException;
import org.apache.maven.wagon.authorization.AuthorizationException;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;


/**
 * s3Wagon, using AWS S3 Connection.
 *
 */
public class s3Wagon extends AbstractWagon 
{
	private volatile AmazonS3Client amazonS3;

	
	protected void getConnection() {
			this.amazonS3 =  (AmazonS3Client) AmazonS3ClientBuilder.defaultClient();
	}


	public void get(String resourceName, File destination)
			throws TransferFailedException, ResourceDoesNotExistException, AuthorizationException {
		try {
			amazonS3.getObject(
	                new GetObjectRequest(getRepository().getBasedir(), resourceName),
	                destination);
			}
		catch (Exception e) {
			throw new ResourceDoesNotExistException(e.getMessage());
		}
	}


	public boolean getIfNewer(String resourceName, File destination, long timestamp)
			throws TransferFailedException, ResourceDoesNotExistException, AuthorizationException {
		ObjectMetadata meta = amazonS3.getObjectMetadata(getRepository().getBasedir(), resourceName);
		return meta.getLastModified().compareTo(new Date(timestamp)) < 0;
	}


	public void put(File source, String destination)
			throws TransferFailedException, ResourceDoesNotExistException, AuthorizationException {
		// TODO Auto-generated method stub
		throw new AuthorizationException("this plugin is not for upload to s3.");
	}


	@Override
	protected void openConnectionInternal() throws ConnectionException, AuthenticationException {
			this.amazonS3 =  (AmazonS3Client) AmazonS3ClientBuilder.defaultClient();
	}


	@Override
	protected void closeConnection() throws ConnectionException {
		this.amazonS3 =  null;
		}
	

}

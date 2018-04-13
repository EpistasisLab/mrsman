package org.epistasis.pennvibe.fhirplug;
import org.hl7.fhir.dstu3.model.Bundle;
import org.hl7.fhir.dstu3.model.Bundle.BundleType;
import org.hl7.fhir.dstu3.model.Bundle.HTTPVerb;
import org.hl7.fhir.dstu3.model.Enumerations;
import org.hl7.fhir.dstu3.model.Observation;
import org.hl7.fhir.dstu3.model.Observation.ObservationStatus;
import org.hl7.fhir.dstu3.model.Patient;
import org.hl7.fhir.dstu3.model.Quantity;
import org.hl7.fhir.dstu3.model.Reference;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.model.primitive.IdDt;
import ca.uhn.fhir.okhttp.client.OkHttpRestfulClientFactory;
import ca.uhn.fhir.rest.client.api.IClientInterceptor;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.api.IRestfulClientFactory;
import ca.uhn.fhir.rest.client.interceptor.BasicAuthInterceptor;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
    
     // Create a patient object
        Patient patient = new Patient();
        patient.addIdentifier()
           .setSystem("http://localhost:8082/openmrs/ws/fhir")
           .setValue("12345");
        patient.addName()
           .setFamily("Jameson")
           .addGiven("J")
           .addGiven("Jonah");
        patient.setGender(Enumerations.AdministrativeGender.MALE);
         
        // Give the patient a temporary UUID so that other resources in
        // the transaction can refer to it
        patient.setId(IdDt.newRandomUuid());
         
        // Create an observation object
        Observation observation = new Observation();
        observation.setStatus(ObservationStatus.FINAL);
        observation
           .getCode()
              .addCoding()
                 .setSystem("http://loinc.org")
                 .setCode("789-8")
                 .setDisplay("Erythrocytes [#/volume] in Blood by Automated count");
        observation.setValue(
           new Quantity()
              .setValue(4.12)
              .setUnit("10 trillion/L")
              .setSystem("http://unitsofmeasure.org")
              .setCode("10*12/L"));
         
        // The observation refers to the patient using the ID, which is already
        // set to a temporary UUID 
        observation.setSubject(new Reference(patient.getId()));
         
        // Create a bundle that will be used as a transaction
        Bundle bundle = new Bundle();
        bundle.setType(BundleType.TRANSACTION);
         
        // Add the patient as an entry. This entry is a POST with an
        // If-None-Exist header (conditional create) meaning that it
        // will only be created if there isn't already a Patient with
        // the identifier 12345
        bundle.addEntry()
           .setFullUrl(patient.getId())
           .setResource(patient)
           .getRequest()
              .setUrl("Patient")
              .setIfNoneExist("identifier=http://localhost:8082/openmrs/ws/fhir|12345")
              .setMethod(HTTPVerb.POST);
         
        // Add the observation. This entry is a POST with no header
        // (normal create) meaning that it will be created even if
        // a similar resource already exists.
        bundle.addEntry()
           .setResource(observation)
           .getRequest()
              .setUrl("Observation")
              .setMethod(HTTPVerb.POST);
         
        // Log the request
        FhirContext ctx = FhirContext.forDstu3();
        
        
       // IRestfulClientFactory clientFactory = ctx.getRestfulClientFactory();
        
     // Create an HTTP basic auth interceptor
     String username = "admin";
     String password = "Admin123";
     IClientInterceptor authInterceptor = new BasicAuthInterceptor(username, password);
     
  // Use OkHttp
     ctx.setRestfulClientFactory(new OkHttpRestfulClientFactory(ctx));
      
     // Create the client
//     IGenericClient client = ctx.newRestfulGenericClient("http://localhost:9999/fhir");
        
        
 //       System.out.println(ctx.newXmlParser().setPrettyPrint(true).encodeResourceToString(bundle));
         
        // Create a client and post the transaction to the server
        IGenericClient client = ctx.newRestfulGenericClient("http://localhost:8082/openmrs/ws/fhir");
        client.registerInterceptor(authInterceptor);
           
        
        Bundle resp = client.transaction().withBundle(bundle).execute();
         
        // Log the response
        System.out.println(ctx.newXmlParser().setPrettyPrint(true).encodeResourceToString(resp));
    
    
    
    }
    
    
    
    
}



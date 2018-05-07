package org.epistasis.pennvibe.fhirplug;
import java.util.Date;

import org.hl7.fhir.dstu3.model.Bundle.BundleType;
import org.hl7.fhir.dstu3.model.Bundle.HTTPVerb;
import org.hl7.fhir.dstu3.model.Enumerations;
import org.hl7.fhir.dstu3.model.Observation;
import org.hl7.fhir.dstu3.model.Observation.ObservationStatus;
import org.hl7.fhir.dstu3.model.Patient;
import org.hl7.fhir.dstu3.model.Period;
import org.hl7.fhir.dstu3.model.Quantity;
import org.hl7.fhir.dstu3.model.Reference;
import org.hl7.fhir.instance.model.api.IBaseBundle;
import org.hl7.fhir.instance.model.api.IIdType;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.model.api.TemporalPrecisionEnum;
import ca.uhn.fhir.model.primitive.IdDt;
import ca.uhn.fhir.okhttp.client.OkHttpRestfulClientFactory;
import ca.uhn.fhir.rest.api.MethodOutcome;
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
    
        Observation observation = new Observation();
        observation
           .getCode()
              .addCoding()
                 .setSystem("http://ciel.org")
                 .setCode("5087")
                 .setDisplay("Pulse");
        observation.setValue(
           new Quantity()
              .setValue(60)
              .setUnit("rate/min")
              .setSystem("http://unitsofmeasure.org")
              .setCode("rate/min"));
        
        Period period = new Period();
    	period.setStart(new Date());
    	period.setEnd(new Date());
    	observation.setEffective(period);
       // observation.setEffective(value)
      //  observation.set
         

        

         
        // Log the request
        FhirContext ctx = FhirContext.forDstu3();
        
        
       // IRestfulClientFactory clientFactory = ctx.getRestfulClientFactory();
        
     // Create an HTTP basic auth interceptor
     String username = "admin";
     String password = "Admin123";
     IClientInterceptor authInterceptor = new BasicAuthInterceptor(username, password);
     
  // Use OkHttp
     ctx.setRestfulClientFactory(new OkHttpRestfulClientFactory(ctx));
      
        IGenericClient client = ctx.newRestfulGenericClient("http://localhost:8082/openmrs/ws/fhir");
        client.registerInterceptor(authInterceptor);
       
        // search for patient 123
    //    Patient patient = client.read()
    //                            .resource(Patient.class)
    //                            .withId("acf17485-eb07-4e80-9e26-43099bd41ab5")
    //                            .execute();
       
      //  observation.set
   //     observation.setSubject(new Reference("Patient/eb552673-a137-4e52-b488-cd662de3e9f5"));
        Reference reference = new Reference();
        reference.setReference("Patient/eb552673-a137-4e52-b488-cd662de3e9f5");
        //observation.setSubject(new Reference(patient.getId()));
        observation.setSubject(reference);
        System.out.println(observation);
        
        MethodOutcome resp = client.create().resource(observation).encodedJson().execute();
        System.out.println(resp);

    
    
    }
    
    
    
    
}



/*	Copyright (c) 2016 Diego Teijeiro, Xoán C. Pardo, Patricia González
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without modification, 
 * are permitted for any purpose (including commercial purposes) provided that the 
 * following conditions are met: 
 * 
 * 1. Redistributions of source code must retain the above copyright notice, this
 *     list of conditions and the following acknowledgments and disclaimer.
 *     
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following acknowledgments and disclaimer 
 *     in the documentation and/or other materials provided with the distribution. 
 *     
 * 3. All publications mentioning features or use of this software are asked to credit
 *     the authors by citing the references provided at: https://bitbucket.org/xcpardo/sipde
 *                                   
 * ACKNOWLEDGMENTS
 * 
 * This work is the result of a collaboration between the Computer Architecture Group (GAC)
 * at Universidade da Coruña (A Coruña, Spain) and the (Bio)Processing Engineering Group
 * at IIM-CSIC (Vigo, Spain). It was produced at the Universidade da Coruña (UDC) and 
 * received financial support from the Spanish Ministerio de Economía y Competitividad 
 * (and the FEDER) through the Project SYN-BIOFACTORY (grant number DPI2014-55276-C5-2-R). 
 * It has been also supported by the Spanish Ministerio de Ciencia e Innovación (and the FEDER)
 * through the Project TIN2013-42148-P, and by the Galician Government (Xunta de Galicia)
 * under the Consolidation Program of Competitive Research Units (Network Ref. R2014/041
 * and Project Ref. GRC2013/055) cofunded by FEDER funds of the EU.
 * 
 * DISCLAIMER
 *  
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" 
 *  WITH NO WARRANTY OF ANY KIND, EITHER EXPRESSED OR IMPLIED. IN NO EVENT SHALL 
 *  THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DAMAGES HOWEVER  
 *  CAUSED AND ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED  
 *  OF THE POSSIBILITY OF SUCH DAMAGE.
 *  
 *  */

/** 
   * *****************************************************************
   *  	File:   StandardMutator.scala
   * 		Author: Diego Teijeiro <diego.teijeiro@udc.es>
   * 		Under direction and supervision by: 
   * 							Xoan C. Pardo <xoan.pardo@udc.gal> and
   * 						  Patricia Gonzalez <patricia.gonzalez@udc.es>
   *
   *   Affiliation:
   *       Grupo de Arquitectura de Computadores (GAC)
   *       Departamento de Electronica e Sistemas
   *       Facultade de Informatica
   *       Universidade da Coruña (UDC)
   *       Spain
   * 
   * 		Created on 2015-08-10
   * 
   * 		Implementation of the DE/rand/1 mutation strategy
   * 
   *  *****************************************************************
   */

package deFunctions.mutators

import deFunctions.Individuo
import deFunctions.DEParameters
import org.apache.commons.math3.random.RandomDataGenerator

/**
 * @author diego
 */
class StandardMutator extends Mutator{
  
  /**
   * Apply the mutation strategy to get a new candidate solution
   * 
   * @param params DE parameters
   * @param fathers List of random individuals to be used for the generation of the new candidate solution
   * @return A new candidate solution obtained by applying the mutation strategy to the list of fathers
   */
  def mutate (params:MutatorParams, fathers:List[Individuo]):Individuo = {
    
    if (fathers.size<fathersNeeded()){
      println("Not enough fathers for mutation.") 
      return null
    }
    
    params match {
      case dem: DEMutatorParams =>{
        var result = new Array[Double](dem.dim)
        val rand:RandomDataGenerator = new RandomDataGenerator()
        
        // Get a random index
        val rInd = rand.nextInt(0, dem.dim-1)

        //calculate new candidate solution
        for (i <- 0 until dem.dim){
          // Get an uniformely distributed random number in (0,1) for each variable
          val r = rand.nextUniform(0, 1)
          // If r<CR or i=rInd calculate a[i] + F*(b[i]-c[i])
          if (r<dem.cr || i==rInd){
            result(i) = fathers(1).values(i) + dem.f*(fathers(2).values(i)-fathers(3).values(i))
          } else {
            result(i) = fathers(0).values(i)
          }
                    
//      // Keep values into border limits
//      if (result(i)>params.upLimit)
//        result(i)=params.upLimit
//      if (result(i)<params.lowLimit)
//        result(i)=params.lowLimit
        }
        
        return new Individuo(result)
      }
      case _ => null
    }
  }
  
  /**
   * Number of fathers needed by the mutation strategy
   * 
   * @return The number of fathers to be used by the mutation strategy
   */
   def fathersNeeded():Int = {return 4}
  
}
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
   *  	File:   AsyncTask.scala
   * 		Author: Xoan C. Pardo <xoan.pardo@udc.gal>
   *
   *   Affiliation:
   *       Grupo de Arquitectura de Computadores (GAC)
   *       Departamento de Electronica e Sistemas
   *       Facultade de Informatica
   *       Universidade da Coruña (UDC)
   *       Spain
   * 
   * 		Created on 2016-03-11
   * 
   * 	 This file contains the high level classes used to abstract the
   *   asynchronous execution of a Local Solver (LS). 
   *   
   *   The LS is concurrently run using a separate thread.
   *   
   *  *****************************************************************
   */

package deFunctions.solvers

import scala.concurrent.Promise
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Promise, Future, Await}
//import scala.concurrent.duration.Duration
import deFunctions.DEParameters
import deFunctions.Individuo
import java.util.concurrent.CancellationException

import java.util.concurrent.Callable
import scala.concurrent.duration._


/**
 * A task that wraps a Callable and gets its result asynchronously using a Promise
 * 
 *  @author xoan
 * 
 * @param task The callable
 */
class AsyncTask[T, U <: Callable[T]](val task:U) extends Serializable {
      
  private var promise:Option[Promise[T]] = None  // A promise to get the future result of the callable
  
  /**
   * Get the task result
   * 
   * @return The result of the task
   * @throws NoSuchElement when promise is None or its value is empty
   * @throws The Future exception if it has failed
   */
  def value: T = {
     promise.get.future.value.get.get
  }
  
  /**
   * Test if the task is running
   * 
   * @return true if the task is running, false otherwise
   */
  def isRunning: Boolean = {
    promise.isDefined && !isCompleted
  }
  
  /**
   * Test if the task is completed
   * 
   * @return true if the task is completed, false otherwise
   * @throws NoSuchElement when promise is None
   */ 
  def isCompleted: Boolean = {
    promise.get.isCompleted
  }
  
  /**
   * Run the task
   * 
   * @throws IllegalStateException if called with a task already running
   */
  def run(): Unit = {
    if (!isRunning) {
      
        promise = Some(Promise[T])
        // Starts the asynchronous run of the task
        val future = Future {  
          promise.get success doTask
        }
    } else 
      throw new IllegalStateException("A task is already running")
  }
  
  /**
   * Abort the task
   * This method blocks until the task is aborted
   * 
   * @throws NoSuchElement when promise is None or its value is empty
   */
  def cancelAndWait: Unit = {
    if (isRunning) {
      promise.get failure (new CancellationException("Task aborted"))
      Await.ready(promise.get.future, Duration.Inf)  
      
// **********  Uncomment only for debugging **********
//      println("###Asynchronous task aborted")
// *********************************************************    
    }
  }
  
  /**
   * Abort the task
   * 
   * @throws NoSuchElement when promise is None or its value is empty
   */
  def cancel: Unit = {
    if (isRunning) {
      promise.get failure (new CancellationException("Task aborted"))
      
// **********  Uncomment only for debugging **********
//      println("###Asynchronous task aborted")
// *********************************************************    
    }
  }  

  /**
   *  Call the callable
   *  
   *  @return the result of the callable
   */
  protected def doTask():T = {
    task call
  }
}

/**
 * An specialization of AsyncTask for running an asynchronous Local Solver
 * 
 * @author xoan
 * 
 * @param solver The Local Solver
 */
class AsyncSolver[T](solver:LocalSolver[T]) extends AsyncTask[T, LocalSolver[T]](solver) {
  
  var init:Option[T] = None  // Initial value to be optimized

  /**
   * Get the initial value
   * 
   * @returns The initial value to be optimized with the Local Solver
   * @throws NoSuchElement if init is None
   */
  def initValue:T = {
       init.get  
  }

  /**
   * Repeat the last run of the Local Solver
   * (this method is a workaround to overwrite the default inherited run that does not have a parameter to pass 
   *  the initial value. Another option would be a do-nothing method)
   * 
   * @throws IllegalStateException if called without having called run(i:T) before
   */
  override def run(): Unit = {
    if (!isCompleted)
      run(initValue)  // Reexecuta a última optimización 
    else
       throw new IllegalStateException("To run the Local Solver for the first time use the run(i:T) method")
  }

  /**
   * Run the Local Solver asynchronously
   * 
   * @param i The initial value to be optimized
   * @throws IllegalStateException if called with a LS already running
   */
  def run(i:T): Unit = {
    if (!isRunning) {
      
      try {
       init = Some(i)  // store the initial value
       super.run()  // start the async task
      } catch {
        case e:UnsatisfiedLinkError => {
          println("Stopping and failing the Local Solver.")
          super.cancel
        }
      }
    } else 
      throw new IllegalStateException("A Local Solver is already running")
  }
  
  /**
   *  Call the Local Solver
   *  
   *  @return the result of executing the Local Solver
   */
    override protected def doTask():T = {
        task optimize initValue
    }
}


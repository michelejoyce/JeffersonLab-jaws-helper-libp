""" 
.. currentmodule:: JAWSAlarm
.. autoclass:: JAWSAlarm
   :synopsis : Consolidated alarm object
.. moduleauthor::Michele Joyce <erb@jlab.org>
"""

from jlab_jaws.avro.subject_schemas.entities import *
from jlab_jaws_helper.JAWSConnection import *


class JAWSAlarm(object) :
   """ This class encapulates properties from all topics 
   """

   def __init__(self,name,msg=None) :
      
      """ 
         .. automethod:: __init__
         .. rubric:: Methods
         
         Create a JAWSAlarm instance
         Parameters: 
            name (str) : name of the alarm
            msg ('cimpl.Message'/None) : topic message
      
      """      
      self.name = name      
      self.config = {}
      
      
      #If the msg is from a topic other than registered alarms,
      #create an alarm to be defined when the registered alarm comes in.
      if (msg == None) : 
         return   
      
      timestamp = get_msg_timestamp(msg)  
      
      self.config = {
         'type' : AlarmStateEnum.Normal,  ##This is really the state
         'registered' : timestamp
      }               
      self._configure_alarm(get_msg_value(msg).__dict__)

   def _configure_alarm(self,config) :
      """ Configure the alarm with the data from a topic
       
         :param config : alarm configuration from topic
         :type config : dict
      
      """     
       
      #Assign each key of the incoming configuration, to the
      #alarm. This is how the alarm is built up from any topic.
      if (config != None) :         
         for key in config :
            self.config[key] = config[key]
      #return
      #Debug purposes
      if (self.get_name() != None) :
         print(self.get_name())
      
         for key in self.config :
            print("  ",key,"=>",self.config[key])  
         print("--") 
         
   def update_active(self,msg) :
      """ Update an alarm from the active-alarms topic
       
          :param msg : topic messge
          :type msg: 'cimpl.Message' (can be None) 
      
      """     
      msginfo = get_msg_value(msg) 
      timestamp = get_msg_timestamp(msg) 
      
      #Get severity and stat properties from an the topic
      #If msginfo is None, the alarm has been cleared     
      clear = {
         'sevr' : None,
         'stat' : None
      }
      if (msginfo != None) :
         dict = msginfo.msg.__dict__
         #include the time that the state changed.
         dict['statechange'] = timestamp      
      else :
         dict = clear
      
      #Update the alarm's configuration
      self._configure_alarm(dict)
      
      
   def update_state(self,msg) :
      """ Update an alarm from the state topic
       
       :param msg : topic messge
       :type msg: 'cimpl.Message' (can be None) 
      
      """     
      msginfo = get_msg_value(msg)     
      timestamp = get_msg_timestamp(msg) 
      
      dict = msginfo.__dict__
      dict['statechange'] = timestamp      
      self._configure_alarm(dict)
      
   #Update a registered alarm.
   def update_alarm(self,msg) :
      """ Update an alarm from the registered-alarms topic
       
       :param msg : topic messge
       :type msg: 'cimpl.Message' (can be None) 
      
      """     
      
      if (msg == None) : ## ***** NEED TO TEST REMOVE REGISTERED 
         return
      
      timestamp = get_msg_timestamp(msg)
      self.config['registered'] = timestamp
      self._configure_alarm(get_msg_value(msg).__dict__)
   
   def get_name(self) :
      """ Get the name of the alarm       
       :returns: name of the alarm (str)      
      """     
      return(self.name)
   
 
   def get_state(self,name=False,value=False) :      
      """ Get the current state of an alarm
          Note: By default this method returns the AlarmStateEnum 
       :param name : return the lower-case string name of the state 
       :param value: return the numeric value of the state
       
      """           
      val = self.get_val('type')
      if (val != None) :
         if (name) :
            return(val.name.lower())
         elif (value) :
            return(val.value.lower())
      return(val)
   
   def get_state_change(self) :
      """ Get the timestamp of the most recent state change
          
       :returns : timestamp (str)
       
      """           
      return(self.get_val('statechange'))

   def get_sevr(self,name=False,value=False) :
      """ Get the severity of an alarm - if SEVR is not 
          applicable, returns "ALARM" 
          Note: By default this method returns the EPICSSEVR.
          
       :param name : return the name of the severity
       :param value: return the numeric value of the severity
       
      """           
      val = self.get_val('sevr')
      if (val != None) :
         if (name) :
            return(val.name)
         else :
            return(val.value) 
      return("ALARM")
      
   def get_category(self,name=False,value=False) :
      """ Get the alarm category
          Note: By default this method returns an AlarmCategory object
          
       :param name : return the lower-case string name of the category
       :param value: return the numeric value of the category
       
      """           
            
      val = self.get_val('category')
      if (val != None) :
         if (name) :
            return(val.name.lower())
         else :
            return(val.value)
      return(val)
   
   def get_location(self,name=False,value=False) :
      """ Get the alarm location
          Note: By default this method returns an AlarmLoction object
          
       :param name : return the lower-case string name of the location
       :param value: return the numeric value of the location
       
      """           
      val = self.get_val('location')
      if (val != None) :
         
         if (name) :
            return(val.name.lower())
         else :
            return(val.value.lower())     
      return(val)
    
   def get_property(self,property,name=False,value=False)
      """ Generic fetcher for property
                    
       :param property : property of interest
       :type property: string
       :param 
       
      """                  
  
   def get_val(self,property,name=True,) :
      """ Generic fetcher for property
                    
       :param property : property of interest
       :type property: string
       
      """                  
      val = None
      if (self.config != None and property in self.config) :
         val = self.config[property]
      return(val)
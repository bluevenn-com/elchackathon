// ----------------------------------------------------------------------------
// Filename:   IQAppELCHackathonView.mc
// Language:   Monkey C
// Purpose:    IQ Application ELC Hackathon View
// Author:     Chris Hares
// Copyright:  2020 Chris Hares - All rights reserved
// Version:  
//             2020-09-27 - Created File
//             2020-09-28 - Post kick-off development
//             2020-09-29 - Day 2 development - more basic features and biometric capture
//             2020-09-30 - Day 3 development - UI improvements with icon blink
//             2020-10-01 - Day 4 development - Add advice messages
//             2020-10-02 - Dey 5 development - Add Vivoactive support
// ----------------------------------------------------------------------------
using Toybox.WatchUi as Ui;
using Toybox.System as Sys;
using Toybox.Graphics as Gfx;
using Toybox.Time;
using Toybox.Time.Gregorian as Calendar;
using Toybox.Communications as Comm;
using Toybox.Sensor;
using Toybox.Lang;
using Toybox.ActivityMonitor as Monitor;
using Toybox.Timer;
using Toybox.Attention;
using Toybox.Position;

class IQAppELCHackathonView extends Ui.View
{
	// -- Operation
	hidden var _timer;
	hidden var _displayMode;
	hidden var _pageIndex;
	hidden var _moodQuestion;
	hidden var _adviceMessage;
	hidden var _lastInteractivity;
	hidden var _blinkOn;
	const PAGE_COUNT = 5;
	
	// -- Icons
	hidden var _iconRibbon;
	hidden var _iconRibbonSmall;
	hidden var _iconLogo;
	hidden var _iconHappy;
	hidden var _iconSad;
	hidden var _iconLike;
	hidden var _iconAdvice;
	hidden var _iconHeartOn;
	hidden var _iconHeartOff;
	hidden var _iconStepsOn;
	hidden var _iconStepsOff;
	hidden var _iconTempOn;
	hidden var _iconTempOff;
	hidden var _iconSp02On;
	hidden var _iconSp02Off;
	hidden var _timeStart;

	// -- Data	
	hidden var _patientID;
	hidden var _lastMood;
	hidden var _lastHR;
	hidden var _lastSp02;
	hidden var _lastSteps;
	hidden var _lastActive;
	hidden var _lastTemp;
	hidden var _lastPosition;
	hidden var _sendCount;
	hidden var _sendStatus;

    function initialize()
    {
        View.initialize();
        _timer = null;
        _iconRibbon = null;
        _iconRibbonSmall = null;
        _iconLogo = null;
        _iconHappy = null;
        _iconSad = null;
        _iconLike = null;
        _iconAdvice = null;
        _iconHeartOn = null;
        _iconHeartOff = null;
        _iconStepsOn = null;
        _iconStepsOff = null;
        _iconTempOn = null;
        _iconTempOff = null;
        _iconSp02On = null;
        _iconSp02Off = null;
        _timeStart = Sys.getTimer();
        _displayMode = 0;
        _pageIndex = 0;
        _patientID = 2000001;
        _lastMood = 0;
        _lastHR = null;
        _lastSp02 = null;
        _lastSteps = null;
        _lastActive = null;
        _lastTemp = null;
        _lastPosition = null;
        _sendCount = 0;
        _sendStatus = 0;
        _moodQuestion = null;
        _adviceMessage = null;
        _lastInteractivity = null;
        _blinkOn = false;
    }

	function isNumeric(value)
	{
		try
		{
			if (value instanceof Lang.String)
			{
				value = value.toNumber();
				return true;
			}
			else
			{
				return true;
			}
		}
		catch(ex)
		{
		}
		return false;
	}

	function valueToString(valueData)
	{
		if (valueData == null)
		{
			return "---";
		}
		else if (isNumeric(valueData))
		{
			return valueData.toNumber().toString();
		}
		else
		{
			return valueData.toString();
		}
	}
	
	function valueToNumber(valueData)
	{
		if (valueData == null)
		{
			return 0;
		}
		else if (isNumeric(valueData))
		{
			return valueData;
		}
		else
		{
			return 0;
		}
	}
	
	function positionToString(valuePosition)
	{
		if (valuePosition == null)
		{
			return "N/A";
		}
		else
		{
			return valuePosition[0].toString() + "," + valuePosition[1].toString();
		}
	}
	
	function pageMovePrevious()
	{
		if (_displayMode == 1)
		{
			_pageIndex--;
			if (_pageIndex == -1)
			{
				_pageIndex = PAGE_COUNT - 1;
			}
		}
		else if (_displayMode == 2)
		{
			_lastMood = 1;
			_displayMode = 1;
			recordEvent(_patientID, "Mood", valueToNumber(_lastMood), valueToNumber(_lastHR), valueToNumber(_lastSp02), valueToNumber(_lastSteps), valueToNumber(_lastActive), valueToNumber(_lastTemp), _lastPosition);
		}
		else if (_displayMode == 3)
		{
			_displayMode = 1;
		}
		Ui.requestUpdate();
	}
	
	function pageMoveNext()
	{
		if (_displayMode == 1)
		{
			_pageIndex++;
			if (_pageIndex == PAGE_COUNT)
			{
				_pageIndex = 0;
			}
		}
		else if (_displayMode == 2)
		{
			_lastMood = 2;
			_displayMode = 1;
			recordEvent(_patientID, "Mood", valueToNumber(_lastMood), valueToNumber(_lastHR), valueToNumber(_lastSp02), valueToNumber(_lastSteps), valueToNumber(_lastActive), valueToNumber(_lastTemp), _lastPosition);
		}
		else if (_displayMode == 3)
		{
			_displayMode = 1;
		}
		Ui.requestUpdate();
	}
	
	function onWebResponse(responseCode, data)
	{
		if (_sendStatus != responseCode)
		{
			Attention.vibrate([new Attention.VibeProfile(50, 500)]);
		}
		_sendStatus = responseCode;
		if (responseCode == 200)
		{
			_sendCount++;
		}
		else
		{
			Sys.println(Lang.format("Response: $1$", [responseCode]));
		}
	}
	
	function dataValid()
	{
		return (_lastPosition != null);
	}
	
	function sendValid()
	{
		return (_sendStatus == 200);
	}
	
	function recordEvent(valuePatientID, valueEvent, valueMood, valueHR, valueSp02, valueSteps, valueActive, valueTemp, valuePosition)
	{
		if (dataValid())
		{
			var valueTimestamp = Time.now().value();
			var params = 
				{
		    		"timestamp" => valueTimestamp,
		    		"patientid" => valuePatientID,
		    		"event" => valueEvent,
		    		"mood" => valueMood, 
		    		"hr" => valueHR,
		    		"sp02" => valueSp02,
		    		"steps" => valueSteps,
		    		"active" => valueActive,
		    		"temp" => valueTemp,
		    		"lat" => valuePosition[0],
		    		"long" => valuePosition[1]
		    	 };
			var options = {
			    :method => Comm.HTTP_REQUEST_METHOD_POST,
			    :headers => { "Content-Type" => Comm.REQUEST_CONTENT_TYPE_JSON } };
		
			Comm.makeWebRequest("https://ko5no239w9.execute-api.eu-west-1.amazonaws.com/API/listener?orgid=ELCHack",
		    	params,
		    	options,
		    	self.method(:onWebResponse));
			Sys.println(Lang.format("Send: $1$", [params]));
	   	}
   	}
   	
   	function moodCheck(question)
   	{
   		if ((_lastInteractivity == null) || (Sys.getTimer() - _lastInteractivity > 30000))
   		{
   			_moodQuestion = question;
   			_lastInteractivity = Sys.getTimer();
   			_displayMode = 2;
			Attention.vibrate([new Attention.VibeProfile(50, 500)]);
			Ui.requestUpdate();
		}
   	}
   	
   	function adviceMessage(message)
   	{
   		if ((_lastInteractivity == null) || (Sys.getTimer() - _lastInteractivity > 30000))
   		{
   			_adviceMessage = message;
   			_lastInteractivity = Sys.getTimer();
   			_displayMode = 3;
			Attention.vibrate([new Attention.VibeProfile(50, 500)]);
			Ui.requestUpdate();
		}
   	}
   	
   	function recordEventTrigger()
   	{
   		if (_lastMood == 1)
   		{
   			adviceMessage(["Consider deep breathing", "to relax"]);
   		}
   		else if (_lastMood == 2)
   		{
   			//adviceMessage(["Take timeout", "to relax"]);
   			adviceMessage(["Don't forget your", "appointment today"]);
   		}
   		else
   		{
   			moodCheck(["Are you ok?"]);
   		}
   	}
	
    function onSensor(sensor_info)
	{
		if(sensor_info.heartRate != null)
		{
			if ((_lastHR != null) && ((sensor_info.heartRate < _lastHR - 20) || (sensor_info.heartRate > _lastHR + 20)))
			{
		   		moodCheck(["Checking your ok?"]);
			} 
			_lastHR = sensor_info.heartRate;
		}
		if(sensor_info.temperature != null)
		{
			if ((_lastTemp != null) && ((sensor_info.temperature < _lastTemp - 2) || (sensor_info.temperature > _lastTemp + 2)))
			{
		   		moodCheck(["Checking your ok?"]);
			} 
			_lastTemp = sensor_info.temperature;
		}
		if ((sensor_info has :oxygenSaturation) && (sensor_info.oxygenSaturation != null))
		{
			if ((_lastSp02 != null) && ((sensor_info.oxygenSaturation < _lastSp02 - 2) || (sensor_info.oxygenSaturation > _lastHR + 2)))
			{
		   		moodCheck(["Checking your ok?"]);
			} 
			_lastSp02 = sensor_info.oxygenSaturation;
		}
	}
	
	function onPosition(position_info)
	{
		if (position_info.position != null)
		{
			_lastPosition = position_info.position.toDegrees();
		}
	}
	
	function timerCallback()
	{
		if ((_displayMode == 0) && dataValid())
		{
			_displayMode = 1;
		}
		else if ((_displayMode == 1) && (_lastInteractivity != null) && (Sys.getTimer() - _lastInteractivity > 60000))
		{
			adviceMessage(["Keep moving to", "help with fitness"]);
		}
		var info = Monitor.getInfo();
		_lastSteps = info.steps;
		_lastActive = info.activeMinutesDay.total;
		recordEvent(_patientID, "Sensor", valueToNumber(_lastMood), valueToNumber(_lastHR), valueToNumber(_lastSp02), valueToNumber(_lastSteps), valueToNumber(_lastActive), valueToNumber(_lastTemp), _lastPosition);
		_blinkOn = !_blinkOn;
		Ui.requestUpdate();
	}

    // Load your resources here
    function onLayout(dc)
    {
    }

    // Called when this View is brought to the foreground. Restore
    // the state of this View and prepare it to be shown. This includes
    // loading resources into memory.
    function onShow()
    {
    	if (Sensor has :SENSOR_PULSE_OXIMETRY)
    	{
	    	Sensor.setEnabledSensors([Sensor.SENSOR_HEARTRATE, Sensor.SENSOR_PULSE_OXIMETRY, Sensor.SENSOR_TEMPERATURE]);
	    }
	    else
	    {
	    	Sensor.setEnabledSensors([Sensor.SENSOR_HEARTRATE, Sensor.SENSOR_TEMPERATURE]);
	    }
        Sensor.enableSensorEvents(method(:onSensor));
    	Position.enableLocationEvents(Position.LOCATION_CONTINUOUS, method(:onPosition));
 		_iconRibbon = Ui.loadResource(Rez.Drawables.BreastCancerRibbon);
 		_iconRibbonSmall = Ui.loadResource(Rez.Drawables.BreastCancerRibbonSmall);
 		_iconLogo = Ui.loadResource(Rez.Drawables.BlueVennLogo);
 		if (Sys.getSystemStats().totalMemory > 60000)
 		{
	 		_iconHappy = Ui.loadResource(Rez.Drawables.EmojiHappy);
	 		_iconSad = Ui.loadResource(Rez.Drawables.EmojiSad);
	 		_iconLike = Ui.loadResource(Rez.Drawables.EmojiLike);
	 		_iconAdvice = Ui.loadResource(Rez.Drawables.Advice);
	 		_iconHeartOn = Ui.loadResource(Rez.Drawables.HeartOn);
	 		_iconHeartOff = Ui.loadResource(Rez.Drawables.HeartOff);
	 		_iconStepsOn = Ui.loadResource(Rez.Drawables.StepsOn);
	 		_iconStepsOff = Ui.loadResource(Rez.Drawables.StepsOff);
	 		_iconTempOn = Ui.loadResource(Rez.Drawables.TempOn);
	 		_iconTempOff = Ui.loadResource(Rez.Drawables.TempOff);
	 		_iconSp02On = Ui.loadResource(Rez.Drawables.Sp02On);
	 		_iconSp02Off = Ui.loadResource(Rez.Drawables.Sp02Off);
 		}
        _timer = new Timer.Timer();
		_timer.start(method(:timerCallback), 1000, true);
    }

    // Update the view
    function onUpdate(dc)
    {
    	var drawX;
    	var drawY;
    	
        // Call the parent onUpdate function to redraw the layout
        View.onUpdate(dc);
        dc.setColor(Gfx.COLOR_BLACK, Gfx.COLOR_WHITE);
        dc.clear();
    	if (_displayMode == 0)
    	{
    		// -- Start Screen
			drawX = (dc.getWidth() -  _iconRibbon.getWidth()) / 2;
			drawY = 25;
			dc.drawBitmap(drawX, drawY, _iconRibbon);
			drawX = (dc.getWidth() - _iconLogo.getWidth()) / 2;
			drawY += _iconRibbon.getHeight();
			dc.drawBitmap(drawX, drawY, _iconLogo);
	        dc.setColor(Gfx.COLOR_BLACK, Gfx.COLOR_TRANSPARENT);
			drawX = dc.getWidth() / 2;
			drawY = (dc.getHeight() - (dc.getFontHeight(Gfx.FONT_LARGE) * 3)) / 2;
			dc.drawText(drawX, drawY, Gfx.FONT_LARGE, "ELC", Gfx.TEXT_JUSTIFY_CENTER);
			drawY += dc.getFontHeight(Gfx.FONT_LARGE);
			dc.drawText(drawX, drawY, Gfx.FONT_LARGE, "Hackathon", Gfx.TEXT_JUSTIFY_CENTER);
			drawY += dc.getFontHeight(Gfx.FONT_LARGE);
			dc.drawText(drawX, drawY, Gfx.FONT_LARGE, "2020", Gfx.TEXT_JUSTIFY_CENTER);
    	}
    	else if (_displayMode == 1)
    	{
    		// -- Watch Look
	    	var value;
	    	var text;
	    	var colour;
	    	var image;
	    	
	        if (_pageIndex == 1)
	        {
	        	text = "HR";
	        	value = valueToString(_lastHR);
	    		colour = Gfx.COLOR_RED;
	    		image = _blinkOn ? _iconHeartOn : _iconHeartOff;
	        }
	        else if (_pageIndex == 2)
	        {
	        	text = "Steps";
	        	value = valueToString(_lastSteps);
	    		colour = Gfx.COLOR_DK_GREEN;
	    		image = _blinkOn ? _iconStepsOn : _iconStepsOff;
	        }
	        else if (_pageIndex == 3)
	        {
	        	text = "Temp";
	        	value = valueToString(_lastTemp);
	    		colour = Gfx.COLOR_ORANGE;
	    		image = _blinkOn ? _iconTempOn : _iconTempOff;
	        }
	        else if (_pageIndex == 4)
	        {
	        	text = "Sp02";
	        	value = valueToString(_lastSp02);
	    		colour = Gfx.COLOR_DK_BLUE;
	    		image = _blinkOn ? _iconSp02On : _iconSp02Off;
	        }
	        else
	        {
	    		var clockTime = Sys.getClockTime();
		        var clockDate = Calendar.info(Time.now(), Time.FORMAT_LONG);
	        
	           	text = Lang.format("$1$ $2$ $3$", [clockDate.day_of_week, clockDate.month, clockDate.day]);
	    		value = Lang.format("$1$:$2$", [clockTime.hour.format("%02d"), clockTime.min.format("%02d")]);
	    		colour = Gfx.COLOR_PINK;
	    		image = _iconRibbonSmall;
	        }
	        dc.setColor((dataValid() && sendValid()) ? colour : Gfx.COLOR_DK_GRAY, Gfx.COLOR_TRANSPARENT);
	        drawX = dc.getWidth() / 2;
	        drawY = (dc.getHeight() / 2) - dc.getFontHeight(Gfx.FONT_NUMBER_THAI_HOT);
	        
	        // -- Adjust for smaller devices
			if (dc.getHeight() <= 240)
			{
				drawY -= 45;
			}
			
			// --- Layout Screen
	        dc.drawText(drawX, drawY, Gfx.FONT_MEDIUM, text, Gfx.TEXT_JUSTIFY_CENTER);
	        drawY += dc.getFontHeight(Gfx.FONT_MEDIUM);
	        dc.drawText(drawX, drawY, Gfx.FONT_NUMBER_THAI_HOT, value, Gfx.TEXT_JUSTIFY_CENTER);
	        if (image != null)
	        {
				drawX = (dc.getWidth() - image.getWidth()) / 2;
				drawY = dc.getHeight() - image.getHeight();
				if (dc.getHeight() <= 240)
				{
					drawY -= 20;
				}
				else
				{
					drawY -= 50;
				}
				dc.drawBitmap(drawX, drawY, image);
			}
		}
		else if (_displayMode == 2)
		{
			// -- Mood Check
			if (_iconHappy != null)
			{
				drawX = (dc.getWidth() - _iconHappy.getWidth()) / 2;
				drawY = 25;
				dc.drawBitmap(drawX, drawY, _iconHappy);
			}
			drawX = dc.getWidth() / 2;
			drawY = (dc.getHeight() - (_moodQuestion.size() * Gfx.getFontHeight(Gfx.FONT_LARGE))) / 2;
	        dc.setColor(Gfx.COLOR_BLACK, Gfx.COLOR_TRANSPARENT);
	        for(var index = 0; index < _moodQuestion.size(); index++)
	        {
				dc.drawText(drawX, drawY, Gfx.FONT_LARGE, _moodQuestion[index], Gfx.TEXT_JUSTIFY_CENTER);
				drawY += Gfx.getFontHeight(Gfx.FONT_LARGE);
			}
			if (_iconSad != null)
			{
				drawX = (dc.getWidth() - _iconSad.getWidth()) / 2;
				drawY = dc.getHeight() - 25 - _iconSad.getHeight();
				dc.drawBitmap(drawX, drawY, _iconSad);
			}
		}
		else
		{
			// -- Advice Message
			drawX = (dc.getWidth() - _iconAdvice.getWidth()) / 2;
			drawY = 25;
			dc.drawBitmap(drawX, drawY, _iconAdvice);
	        dc.setColor(Gfx.COLOR_BLACK, Gfx.COLOR_TRANSPARENT);
			drawX = dc.getWidth() / 2;
			drawY = (dc.getHeight() - (_adviceMessage.size() * Gfx.getFontHeight(Gfx.FONT_LARGE))) / 2;
	        dc.setColor(Gfx.COLOR_BLACK, Gfx.COLOR_TRANSPARENT);
	        for(var index = 0; index < _adviceMessage.size(); index++)
	        {
				dc.drawText(drawX, drawY, Gfx.FONT_LARGE, _adviceMessage[index], Gfx.TEXT_JUSTIFY_CENTER);
				drawY += Gfx.getFontHeight(Gfx.FONT_LARGE);
			}
			if (_iconLike != null)
			{
				drawX = (dc.getWidth() - _iconLike.getWidth()) / 2;
				drawY = dc.getHeight() - 25 - _iconLike.getHeight();
				dc.drawBitmap(drawX, drawY, _iconLike);
			}
		}
    }

    // Called when this View is removed from the screen. Save the
    // state of this View here. This includes freeing resources from
    // memory.
    function onHide()
    {
    	_iconRibbon = null;
    	_iconRibbonSmall = null;
    	_iconLogo = null;
    	_iconHappy = null;
    	_iconSad = null;
    	_iconLike = null;
    	_iconHeartOn = null;
    	_iconHeartOff = null;
    	_iconStepsOn = null;
    	_iconStepsOff = null;
    	_iconTempOn = null;
    	_iconTempOff = null;
    	_iconSp02On = null;
    	_iconSp02Off = null;
        Sensor.enableSensorEvents(null);
	    Sensor.setEnabledSensors([]);
		Position.enableLocationEvents(Position.LOCATION_DISABLE, null);
    }
}

class IQAppELCHackathonDelegate extends Ui.BehaviorDelegate
{
	hidden var _view;
	hidden var _backCounter;
	hidden var _eventTimer;
	
	function initialize(view)
	{
        BehaviorDelegate.initialize();
		_view = view;
		_backCounter = 0;
		_eventTimer = null;
	}
	
	function timerEventCallback()
	{
		_backCounter = 0;
		_eventTimer = null;
	}

	function onPreviousPage()
	{
		_view.pageMovePrevious();
		return true;
	}
	
	function onNextPage()
	{
		_view.pageMoveNext();
		return true;
	}

	function onTap(evt)
	{
		if (evt.getCoordinates() != null)
		{
			var location = evt.getCoordinates();
			if (location[1] > (Sys.getDeviceSettings().screenHeight / 2))
			{
				_view.pageMovePrevious();
				return true;				
			}
			else
			{
				_view.pageMoveNext();
				return true;
			}
		}
		return false;
	}
	
	function onKey(evt)
	{
		if ((evt.getKey() == Ui.KEY_ENTER) || (evt.getKey() == Ui.KEY_START)) 
		{
			_view.recordEventTrigger();
			return true;
		}
		else if (evt.getKey() == Ui.KEY_MODE)
		{
			_view.pageMoveNext();
			return true;
		}
		return false;
	}

	function onBack()
	{
		if (_backCounter >= 2)
		{
			return false;
		}
		else
		{
			_backCounter++;
			if (_eventTimer != null)
			{
				_eventTimer.stop();
    			_eventTimer = null;
			}
			_eventTimer = new Timer.Timer();
			_eventTimer.start(method(:timerEventCallback), 5000, false);
			return true;
		}
	}
}

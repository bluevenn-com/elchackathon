// ----------------------------------------------------------------------------
// Filename:   IQAppELCHackathonApp.mc
// Language:   Monkey C
// Purpose:    IQ Application ELC Hackathon App
// Author:     Chris Hares
// Copyright:  2020 Chris Hares - All rights reserved
// Version:  
//             2020-09-27 - Created File
// ----------------------------------------------------------------------------
using Toybox.Application;
using Toybox.WatchUi;

class IQAppELCHackathonApp extends Application.AppBase
{
    function initialize() 
    {
        AppBase.initialize();
    }

    // onStart() is called on application start up
    function onStart(state)
    {
    }

    // onStop() is called when your application is exiting
    function onStop(state)
    {
    }

    // Return the initial view of your application here
    function getInitialView()
    {
    	var view = new IQAppELCHackathonView();
    	var delegate = new IQAppELCHackathonDelegate(view);
        return [ view, delegate ];
    }
}

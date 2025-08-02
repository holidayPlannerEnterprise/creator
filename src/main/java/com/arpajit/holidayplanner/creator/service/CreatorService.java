package com.arpajit.holidayplanner.creator.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.arpajit.holidayplanner.creator.CreatorDataServComm;

@Service
public class CreatorService {
    @Autowired
    private CreatorDataServComm dataService;

    public String getAllHolidayDetails() throws Exception {
        return dataService.allHolidayDetails();
    }
}

package com.linkedin.thirdeye.dashboard.resources.v2;

import com.linkedin.thirdeye.anomaly.events.DefaultDeploymentEventProvider;
import com.linkedin.thirdeye.anomaly.events.DefaultHolidayEventProvider;
import com.linkedin.thirdeye.anomaly.events.EventDataProviderManager;
import com.linkedin.thirdeye.anomaly.events.EventFilter;
import com.linkedin.thirdeye.anomaly.events.EventType;
import com.linkedin.thirdeye.anomaly.events.HistoricalAnomalyEventProvider;
import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.datalayer.bao.EventManager;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path(value = "/event")
@Produces(MediaType.APPLICATION_JSON)
public class EventResource {
  private static final Logger LOG = LoggerFactory.getLogger(EventResource.class);

  static final EventDataProviderManager EVENT_DATA_PROVIDER_MANAGER =  EventDataProviderManager.getInstance();
  static final EventManager eventDAO = DAORegistry.getInstance().getEventDAO();

  public EventResource(String informedAPIUrl) {
    EVENT_DATA_PROVIDER_MANAGER
        .registerEventDataProvider(EventType.HOLIDAY, new DefaultHolidayEventProvider());
    EVENT_DATA_PROVIDER_MANAGER
        .registerEventDataProvider(EventType.DEPLOYMENT, new DefaultDeploymentEventProvider(informedAPIUrl));
    EVENT_DATA_PROVIDER_MANAGER.registerEventDataProvider(EventType.HISTORICAL_ANOMALY,
        new HistoricalAnomalyEventProvider());
  }

  @GET
  @Path ("/{start}/{end}")
  public List<EventDTO> getHolidayEventsByTime(@PathParam("start") long start,
      @PathParam("end") long end) {
    EventFilter eventFilter = new EventFilter();
    eventFilter.setStartTime(start);
    eventFilter.setEndTime(end);
    eventFilter.setEventType(EventType.HOLIDAY);
    return EVENT_DATA_PROVIDER_MANAGER.getEvents(eventFilter);
  }

  @POST
  @Path("/filter")
  public List<EventDTO> getEventsByFilter(EventFilter eventFilter) {
    return EVENT_DATA_PROVIDER_MANAGER.getEvents(eventFilter);
  }

  @POST
  @Path("/upload")
  public Response uploadEvents(List<EventDTO> eventDTOs) {
    try {
      for (EventDTO e : eventDTOs) {
        List<EventDTO> dupes = eventDAO
            .findEventsBetweenTimeRangeByName(e.getEventType(), e.getName(), e.getStartTime(),
                e.getEndTime());

        if (dupes.size() > 0) {
          LOG.info("Found duplicate events, skipping {}", e.toString());
        } else {
          eventDAO.save(e);
        }
      }
    } catch (Exception e) {
      LOG.error("Could not ingest the event :" + e.toString(), e);
      return Response.serverError().build();
    }
    return Response.ok().build();
  }
}

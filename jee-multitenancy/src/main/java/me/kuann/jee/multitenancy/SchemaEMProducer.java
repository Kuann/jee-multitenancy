package me.kuann.jee.multitenancy;

import java.sql.Connection;
import java.sql.SQLException;

import javax.enterprise.context.RequestScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import org.hibernate.Session;
import org.hibernate.jdbc.Work;

@RequestScoped
@Transactional
public class SchemaEMProducer {

	@PersistenceContext
	private EntityManager em;
	
	@Inject
	private HttpServletRequest request;
	
	@Produces
	@CurrentSchema
	@RequestScoped
	public EntityManager create() {
		String tenant = request.getHeader("tenant");
		if (null == tenant) {
			return getPublicEntityManager();
		} else {
			return getTenantEntityManager(tenant);
		}
	}

	private EntityManager getPublicEntityManager() {
		Session hibernateSession = em.unwrap(Session.class);
		
		hibernateSession.doWork(new Work() {
			@Override
			public void execute(Connection connection) throws SQLException {
				try {
					connection.createStatement().execute("RESET ROLE");
					connection.createStatement().execute("SET ROLE public_role");
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
			}
		});
		return em;
	}
	
	private EntityManager getTenantEntityManager(String tenant) {
		Session hibernateSession = em.unwrap(Session.class);
		
		hibernateSession.doWork(new Work() {
			@Override
			public void execute(Connection connection) throws SQLException {
				try {
					connection.createStatement().execute("RESET ROLE");
					connection.createStatement().execute("SET ROLE " + tenant);
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
			}
		});
		return em;
	}
	
}

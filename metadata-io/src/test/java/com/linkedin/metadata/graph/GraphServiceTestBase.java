package com.linkedin.metadata.graph;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.RelationshipDirection;
import com.linkedin.metadata.query.RelationshipFilter;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static com.linkedin.metadata.dao.utils.QueryUtils.EMPTY_FILTER;
import static com.linkedin.metadata.dao.utils.QueryUtils.newFilter;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


/**
 * Base class for testing any GraphService implementation.
 * Derive the test class from this base and get your GraphService implementation
 * tested with all these tests.
 *
 * You can add implementation specific tests in derived classes, or add general tests
 * here and have all existing implementations tested in the same way.
 *
 * Note this base class does not test GraphService.addEdge explicitly. This method is tested
 * indirectly by all tests via `getPopulatedGraphService` at the beginning of each test.
 * The `getPopulatedGraphService` method calls `GraphService.addEdge` to populate the Graph.
 * Feel free to add a test to your test implementation that calls `getPopulatedGraphService` and
 * asserts the state of the graph in an implementation specific way.
 */
abstract public class GraphServiceTestBase {

  /**
   * Some test dataset types.
   */
  protected static String datasetType = "dataset";
  protected static String datasetVersionType = "datasetVersion";

  /**
   * Some test datasets.
   */
  protected static String datasetOneUrnString = "urn:li:" + datasetType + ":(urn:li:dataPlatform:type,SampleDatasetOne,PROD)";
  protected static String datasetTwoUrnString = "urn:li:" + datasetType + ":(urn:li:dataPlatform:type,SampleDatasetTwo,PROD)";
  protected static String datasetThreeUrnString = "urn:li:" + datasetVersionType + ":(urn:li:dataPlatform:type,SampleVersionedDataset,PROD,V1)";
  protected static String datasetFourUrnString = "urn:li:" + datasetVersionType + ":(urn:li:dataPlatform:type,SampleVersionedDataset,PROD,V2)";

  protected static Urn datasetOneUrn = createFromString(datasetOneUrnString);
  protected static Urn datasetTwoUrn = createFromString(datasetTwoUrnString);
  protected static Urn datasetThreeUrn = createFromString(datasetThreeUrnString);
  protected static Urn datasetFourUrn = createFromString(datasetFourUrnString);

  /**
   * Some test relationships.
   */
  protected static String upstreamOf = "UpstreamOf";
  protected static String nextVersionOf = "NextVersionOf";

  /**
   * Some relationship filters.
   */
  protected static RelationshipFilter incomingRelationships = createRelationshipFilter(RelationshipDirection.INCOMING);
  protected static RelationshipFilter outgoingRelationships = createRelationshipFilter(RelationshipDirection.OUTGOING);
  protected static RelationshipFilter undirectedRelationships = createRelationshipFilter(RelationshipDirection.UNDIRECTED);

  /**
   * Any source and destination type value.
   */
  protected static String anyType = "";

  @Test
  public void testStaticUrns() {
    assertNotNull(datasetOneUrn);
    assertNotNull(datasetTwoUrn);
    assertNotNull(datasetThreeUrn);
    assertNotNull(datasetFourUrn);
  }

  /**
   * Provides the current GraphService instance to test. This is being called by the test method
   * at most once. The serviced graph should be empty.
   *
   * @return the GraphService instance to test
   * @throws Exception on failure
   */
  @Nonnull
  abstract protected GraphService getGraphService() throws Exception;

  /**
   * Allows the specific GraphService test implementation to wait for GraphService writes to
   * be synced / become available to reads.
   *
   * @throws Exception on failure
   */
  abstract protected void syncAfterWrite() throws Exception;

  /**
   * Calls getGraphService to retrieve the test GraphService and populates it
   * with edges via `GraphService.addEdge`.
   *
   * @return test GraphService
   * @throws Exception on failure
   */
  protected GraphService getPopulatedGraphService() throws Exception {
    GraphService service = getGraphService();

    List<Edge> edges = Arrays.asList(
            new Edge(datasetOneUrn, datasetTwoUrn, upstreamOf),
            new Edge(datasetTwoUrn, datasetThreeUrn, upstreamOf),
            new Edge(datasetTwoUrn, datasetFourUrn, upstreamOf),
            new Edge(datasetFourUrn, datasetThreeUrn, nextVersionOf)
    );

    edges.forEach(service::addEdge);
    syncAfterWrite();

    return service;
  }

  protected static RelationshipFilter createRelationshipFilter(RelationshipDirection direction) {
    return createRelationshipFilter(EMPTY_FILTER, direction);
  }

  protected static RelationshipFilter createRelationshipFilter(@Nonnull Filter filter,
                                                               @Nonnull RelationshipDirection direction) {
    return new RelationshipFilter()
            .setCriteria(filter.getCriteria())
            .setDirection(direction);
  }

  protected static @Nullable
  Urn createFromString(@Nonnull String rawUrn) {
    try {
      return Urn.createFromString(rawUrn);
    } catch (URISyntaxException e) {
      return null;
    }
  }

  protected static <T> void assertEqualsNoOrder(List<T> actual, List<T> expected) {
    // TODO: there should be no duplicates
    //Assert.assertEqualsNoOrder(actual.toArray(), expected.toArray());
    assertEquals(new HashSet<>(actual), new HashSet<>(expected));
  }

  @DataProvider(name="FindRelatedUrnsSourceEntityFilterTests")
  public Object[][] getFindRelatedUrnsSourceEntityFilterTests() {
    return new Object[][] {
            new Object[] {
                    newFilter("urn", datasetTwoUrnString),
                    outgoingRelationships,
                    Arrays.asList(datasetThreeUrnString, datasetFourUrnString)
            },
            new Object[] {
                    newFilter("urn", datasetTwoUrnString),
                    incomingRelationships,
                    Arrays.asList(datasetOneUrnString)
            },
            new Object[] {
                    newFilter("urn", datasetTwoUrnString),
                    undirectedRelationships,
                    Arrays.asList(datasetOneUrnString, datasetThreeUrnString, datasetFourUrnString)
            }
    };
  }

  @Test(dataProvider="FindRelatedUrnsSourceEntityFilterTests")
  public void testFindRelatedUrnsSourceEntityFilter(Filter sourceEntityFilter,
                                                    RelationshipFilter relationships,
                                                    List<String> expectedUrnStrings) throws Exception {
    doTestFindRelatedUrns(
            sourceEntityFilter,
            EMPTY_FILTER,
            relationships,
            expectedUrnStrings.toArray(new String[0])
    );
  }

  @DataProvider(name="FindRelatedUrnsDestinationEntityFilterTests")
  public Object[][] getFindRelatedUrnsDestinationEntityFilterTests() {
    return new Object[][] {
            new Object[] {
                    newFilter("urn", datasetOneUrnString),
                    outgoingRelationships,
                    Arrays.asList()
            },
            new Object[] {
                    newFilter("urn", datasetOneUrnString),
                    incomingRelationships,
                    Arrays.asList(datasetOneUrnString)
            },
            new Object[] {
                    newFilter("urn", datasetOneUrnString),
                    undirectedRelationships,
                    Arrays.asList(datasetOneUrnString)
            }
    };
  }

  @Test(dataProvider="FindRelatedUrnsDestinationEntityFilterTests")
  public void testFindRelatedUrnsDestinationEntityFilter(Filter destinationEntityFilter,
                                                         RelationshipFilter relationships,
                                                         List<String> expectedUrnStrings) throws Exception {
    doTestFindRelatedUrns(
            EMPTY_FILTER,
            destinationEntityFilter,
            relationships,
            expectedUrnStrings.toArray(new String[0])
    );
  }

  private void doTestFindRelatedUrns(
          Filter sourceEntityFilter,
          Filter destinationEntityFilter,
          RelationshipFilter relationshipFilter,
          String... expectedUrnStrings
  ) throws Exception {
    doTestFindRelatedUrns(
            sourceEntityFilter, destinationEntityFilter,
            Arrays.asList(upstreamOf), relationshipFilter,
            expectedUrnStrings
    );
  }

  private void doTestFindRelatedUrns(
          final Filter sourceEntityFilter,
          final Filter destinationEntityFilter,
          List<String> relationshipTypes,
          final RelationshipFilter relationshipFilter,
          String... expectedUrnStrings
  ) throws Exception {
    GraphService service = getPopulatedGraphService();

    List<String> relatedUrns = service.findRelatedUrns(
            anyType, sourceEntityFilter,
            anyType, destinationEntityFilter,
            relationshipTypes, relationshipFilter,
            0, 10
    );

    assertEqualsNoOrder(relatedUrns, Arrays.asList(expectedUrnStrings));
  }

  @DataProvider(name="FindRelatedUrnsSourceTypeTests")
  public Object[][] getFindRelatedUrnsSourceTypeTests() {
    return new Object[][]{
            new Object[]{datasetType, outgoingRelationships, Arrays.asList(datasetTwoUrnString, datasetThreeUrnString, datasetFourUrnString)},
            new Object[]{datasetVersionType, outgoingRelationships, Arrays.asList()},

            new Object[]{datasetType, incomingRelationships, Arrays.asList(datasetOneUrnString, datasetTwoUrnString)},
            new Object[]{datasetVersionType, incomingRelationships, Arrays.asList(datasetTwoUrnString)},

            new Object[]{datasetType, undirectedRelationships, Arrays.asList(datasetOneUrnString, datasetTwoUrnString)},
            new Object[]{datasetVersionType, undirectedRelationships, Arrays.asList(datasetThreeUrnString, datasetFourUrnString)}
    };
  }

  @Test(dataProvider="FindRelatedUrnsSourceTypeTests")
  public void testFindRelatedUrnsSourceType(String datasetType,
                                            RelationshipFilter relationships,
                                            List<String> expectedUrnStrings) throws Exception {
    doTestFindRelatedUrns(
            datasetType,
            anyType,
            relationships,
            expectedUrnStrings.toArray(new String[0])
    );
  }

  @DataProvider(name="FindRelatedUrnsDestinationTypeTests")
  public Object[][] getFindRelatedUrnsDestinationTypeTests() {
    return new Object[][] {
            new Object[] { datasetType, outgoingRelationships, Arrays.asList(datasetTwoUrnString) },
            new Object[] { datasetVersionType, outgoingRelationships, Arrays.asList(datasetThreeUrnString, datasetFourUrnString) },

            new Object[] { datasetType, incomingRelationships, Arrays.asList(datasetOneUrnString, datasetTwoUrnString) },
            new Object[] { datasetVersionType, incomingRelationships, Arrays.asList() },

            new Object[] { datasetType, undirectedRelationships, Arrays.asList(datasetOneUrnString, datasetTwoUrnString) },
            new Object[] { datasetVersionType, undirectedRelationships, Arrays.asList(datasetThreeUrnString, datasetFourUrnString) }
    };
  }

  @Test(dataProvider="FindRelatedUrnsDestinationTypeTests")
  public void testFindRelatedUrnsDestinationType(String datasetType,
                                                 RelationshipFilter relationships,
                                                 List<String> expectedUrnStrings) throws Exception {
    doTestFindRelatedUrns(
            anyType,
            datasetType,
            relationships,
            expectedUrnStrings.toArray(new String[0])
    );
  }

  private void doTestFindRelatedUrns(
          final String sourceType,
          final String destinationType,
          final RelationshipFilter relationshipFilter,
          String... expectedUrnStrings
  ) throws Exception {
    GraphService service = getPopulatedGraphService();

    List<String> relatedUrns = service.findRelatedUrns(
            sourceType, EMPTY_FILTER,
            destinationType, EMPTY_FILTER,
            Arrays.asList(upstreamOf), relationshipFilter,
            0, 10
    );

    assertEqualsNoOrder(relatedUrns, Arrays.asList(expectedUrnStrings));
  }

  @Test
  public void testFindRelatedUrnsOffsetAndCount() throws Exception {
    GraphService service = getPopulatedGraphService();

    List<String> firstRelatedUrn = service.findRelatedUrns(
            anyType, newFilter("urn", datasetTwoUrnString),
            anyType, EMPTY_FILTER,
            Arrays.asList(upstreamOf), outgoingRelationships,
            0, 1
    );

    Assert.assertEquals(firstRelatedUrn.size(), 1);
    Assert.assertTrue(firstRelatedUrn.contains(datasetThreeUrnString) || firstRelatedUrn.contains(datasetFourUrnString));

    List<String> secondRelatedUrn = service.findRelatedUrns(
            anyType, newFilter("urn", datasetTwoUrnString),
            anyType, EMPTY_FILTER,
            Arrays.asList(upstreamOf), outgoingRelationships,
            1, 1
    );

    Assert.assertEquals(secondRelatedUrn.size(), 1);
    Assert.assertTrue(secondRelatedUrn.contains(datasetThreeUrnString) || secondRelatedUrn.contains(datasetFourUrnString));
    Assert.assertNotEquals(firstRelatedUrn.get(0), secondRelatedUrn.get(0));
  }

  @DataProvider(name="RemoveEdgesFromNodeTests")
  public Object[][] getRemoveEdgesFromNodeTests() {
    return new Object[][] {
            new Object[] {
                    datasetTwoUrn,
                    outgoingRelationships,
                    Arrays.asList(datasetOneUrnString, datasetThreeUrnString, datasetFourUrnString),
                    Arrays.asList(datasetOneUrnString)
            },
            new Object[] {
                    datasetTwoUrn,
                    incomingRelationships,
                    Arrays.asList(datasetOneUrnString, datasetThreeUrnString, datasetFourUrnString),
                    Arrays.asList(datasetThreeUrnString, datasetFourUrnString)
            },
            new Object[] {
                    datasetTwoUrn,
                    undirectedRelationships,
                    Arrays.asList(datasetOneUrnString, datasetThreeUrnString, datasetFourUrnString),
                    Arrays.asList()
            }
    };
  }

  @Test(dataProvider="RemoveEdgesFromNodeTests")
  public void testRemoveEdgesFromNode(@Nonnull Urn nodeToRemoveFrom,
                                      @Nonnull RelationshipFilter relationshipFilter,
                                      List<String> expectedRelatedUrnsBeforeRemove,
                                      List<String> expectedRelatedUrnsAfterRemove) throws Exception {
    GraphService service = getPopulatedGraphService();
    List<String> edgeTypes = Arrays.asList(upstreamOf);

    List<String> actualRelatedUrnsBeforeRemove = service.findRelatedUrns(
            anyType, newFilter("urn", nodeToRemoveFrom.toString()),
            anyType, EMPTY_FILTER,
            edgeTypes, undirectedRelationships,
            0, 10);
    assertEqualsNoOrder(actualRelatedUrnsBeforeRemove, expectedRelatedUrnsBeforeRemove);

    service.removeEdgesFromNode(
            nodeToRemoveFrom,
            edgeTypes,
            relationshipFilter
    );
    syncAfterWrite();

    List<String> actualRelatedUrnsAfterRemove = service.findRelatedUrns(
            anyType, newFilter("urn", nodeToRemoveFrom.toString()),
            anyType, EMPTY_FILTER,
            edgeTypes, undirectedRelationships,
            0, 10);
    assertEqualsNoOrder(actualRelatedUrnsAfterRemove, expectedRelatedUrnsAfterRemove);
  }

  @Test
  public void testRemoveNode() throws Exception {
    GraphService service = getPopulatedGraphService();

    // assert the initial graph: check all nodes related to upstreamOf and nextVersionOf edges
    assertEqualsNoOrder(
            service.findRelatedUrns(
                    datasetType, EMPTY_FILTER,
                    anyType, EMPTY_FILTER,
                    Arrays.asList(upstreamOf), undirectedRelationships,
                    0, 10
            ),
            Arrays.asList(datasetOneUrnString, datasetTwoUrnString, datasetThreeUrnString, datasetFourUrnString)
    );
    assertEqualsNoOrder(
            service.findRelatedUrns(
                    datasetVersionType, EMPTY_FILTER,
                    anyType, EMPTY_FILTER,
                    Arrays.asList(nextVersionOf), undirectedRelationships,
                    0, 10
            ),
            Arrays.asList(datasetThreeUrnString, datasetFourUrnString)
    );

    service.removeNode(datasetTwoUrn);
    syncAfterWrite();

    // assert the modified graph: check all nodes related to upstreamOf and nextVersionOf edges again
    assertEqualsNoOrder(
            service.findRelatedUrns(
                    datasetType, EMPTY_FILTER,
                    anyType, EMPTY_FILTER,
                    Arrays.asList(upstreamOf), undirectedRelationships,
                    0, 10
            ),
            Collections.emptyList()
    );
    assertEqualsNoOrder(
            service.findRelatedUrns(
                    datasetVersionType, EMPTY_FILTER,
                    anyType, EMPTY_FILTER,
                    Arrays.asList(nextVersionOf), undirectedRelationships,
                    0, 10
            ),
            Arrays.asList(datasetThreeUrnString, datasetFourUrnString)
    );
  }

  @Test
  public void testClear() throws Exception {
    GraphService service = getPopulatedGraphService();

    // assert the initial graph: check all nodes related to upstreamOf and nextVersionOf edges
    assertEqualsNoOrder(
            service.findRelatedUrns(
                    datasetType, EMPTY_FILTER,
                    anyType, EMPTY_FILTER,
                    Arrays.asList(upstreamOf), undirectedRelationships,
                    0, 10
            ),
            Arrays.asList(datasetOneUrnString, datasetTwoUrnString, datasetThreeUrnString, datasetFourUrnString)
    );
    assertEqualsNoOrder(
            service.findRelatedUrns(
                    datasetVersionType, EMPTY_FILTER,
                    anyType, EMPTY_FILTER,
                    Arrays.asList(nextVersionOf), undirectedRelationships,
                    0, 10
            ),
            Arrays.asList(datasetThreeUrnString, datasetFourUrnString)
    );

    service.clear();
    syncAfterWrite();

    // assert the modified graph: check all nodes related to upstreamOf and nextVersionOf edges again
    assertEqualsNoOrder(
            service.findRelatedUrns(
                    datasetType, EMPTY_FILTER,
                    anyType, EMPTY_FILTER,
                    Arrays.asList(upstreamOf), undirectedRelationships,
                    0, 10
            ),
            Collections.emptyList()
    );
    assertEqualsNoOrder(
            service.findRelatedUrns(
                    datasetVersionType, EMPTY_FILTER,
                    anyType, EMPTY_FILTER,
                    Arrays.asList(nextVersionOf), undirectedRelationships,
                    0, 10
            ),
            Arrays.asList()
    );
  }

}

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
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.linkedin.metadata.dao.utils.QueryUtils.EMPTY_FILTER;
import static com.linkedin.metadata.dao.utils.QueryUtils.newFilter;
import static org.testng.Assert.*;


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
   * Some test URN types.
   */
  protected static String datasetType = "dataset";
  protected static String userType = "user";

  /**
   * Some test datasets.
   */
  protected static String datasetOneUrnString = "urn:li:" + datasetType + ":(urn:li:dataPlatform:type,SampleDatasetOne,PROD)";
  protected static String datasetTwoUrnString = "urn:li:" + datasetType + ":(urn:li:dataPlatform:type,SampleDatasetTwo,PROD)";
  protected static String datasetThreeUrnString = "urn:li:" + datasetType + ":(urn:li:dataPlatform:type,SampleDatasetThree,PROD)";
  protected static String datasetFourUrnString = "urn:li:" + datasetType + ":(urn:li:dataPlatform:type,SampleDatasetFour,PROD)";

  protected static Urn datasetOneUrn = createFromString(datasetOneUrnString);
  protected static Urn datasetTwoUrn = createFromString(datasetTwoUrnString);
  protected static Urn datasetThreeUrn = createFromString(datasetThreeUrnString);
  protected static Urn datasetFourUrn = createFromString(datasetFourUrnString);

  /**
   * Some dataset owners.
   */
  protected static String userOneUrnString = "urn:li:" + userType + ":(urn:li:user:system,Ingress,PROD)";
  protected static String userTwoUrnString = "urn:li:" + userType + ":(urn:li:user:individual,UserA,DEV)";

  protected static Urn userOneUrn = createFromString(userOneUrnString);
  protected static Urn userTwoUrn = createFromString(userTwoUrnString);

  /**
   * Some test relationships.
   */
  protected static String downstreamOf = "DownstreamOf";
  protected static String hasOwner = "hasOwner";
  protected static Set<String> allRelationshipTypes = new HashSet<>(Arrays.asList(downstreamOf, hasOwner));

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

    assertNotNull(userOneUrn);
    assertNotNull(userTwoUrn);
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
            new Edge(datasetTwoUrn, datasetOneUrn, downstreamOf),
            new Edge(datasetThreeUrn, datasetTwoUrn, downstreamOf),
            new Edge(datasetFourUrn, datasetTwoUrn, downstreamOf),

            new Edge(datasetOneUrn, userOneUrn, hasOwner),
            new Edge(datasetTwoUrn, userOneUrn, hasOwner),
            new Edge(datasetThreeUrn, userTwoUrn, hasOwner),
            new Edge(datasetFourUrn, userTwoUrn, hasOwner)
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

  protected static <T> void assertEqualsAsSets(List<T> actual, List<T> expected) {
    // TODO: there should be no duplicates
    assertEquals(new HashSet<>(actual), new HashSet<>(expected));
  }

  @DataProvider(name="FindRelatedUrnsSourceEntityFilterTests")
  public Object[][] getFindRelatedUrnsSourceEntityFilterTests() {
    return new Object[][] {
            new Object[] {
                    newFilter("urn", datasetTwoUrnString),
                    Arrays.asList(downstreamOf),
                    outgoingRelationships,
                    Arrays.asList(datasetOneUrnString)
            },
            new Object[] {
                    newFilter("urn", datasetTwoUrnString),
                    Arrays.asList(downstreamOf),
                    incomingRelationships,
                    Arrays.asList(datasetThreeUrnString, datasetFourUrnString)
            },
            new Object[] {
                    newFilter("urn", datasetTwoUrnString),
                    Arrays.asList(downstreamOf),
                    undirectedRelationships,
                    Arrays.asList(datasetOneUrnString, datasetThreeUrnString, datasetFourUrnString)
            },

            new Object[] {
                    newFilter("urn", datasetTwoUrnString),
                    Arrays.asList(hasOwner),
                    outgoingRelationships,
                    Arrays.asList(userOneUrnString)
            },
            new Object[] {
                    newFilter("urn", datasetTwoUrnString),
                    Arrays.asList(hasOwner),
                    incomingRelationships,
                    Arrays.asList()
            },
            new Object[] {
                    newFilter("urn", datasetTwoUrnString),
                    Arrays.asList(hasOwner),
                    undirectedRelationships,
                    Arrays.asList(userOneUrnString)
            },

            new Object[] {
                    newFilter("urn", userOneUrnString),
                    Arrays.asList(hasOwner),
                    outgoingRelationships,
                    Arrays.asList()
            },
            new Object[] {
                    newFilter("urn", userOneUrnString),
                    Arrays.asList(hasOwner),
                    incomingRelationships,
                    Arrays.asList(datasetOneUrnString, datasetTwoUrnString)
            },
            new Object[] {
                    newFilter("urn", userOneUrnString),
                    Arrays.asList(hasOwner),
                    undirectedRelationships,
                    Arrays.asList(datasetOneUrnString, datasetTwoUrnString)
            }
    };
  }

  @Test(dataProvider="FindRelatedUrnsSourceEntityFilterTests")
  public void testFindRelatedUrnsSourceEntityFilter(Filter sourceEntityFilter,
                                                    List<String> relationshipTypes,
                                                    RelationshipFilter relationships,
                                                    List<String> expectedUrnStrings) throws Exception {
    doTestFindRelatedUrns(
            sourceEntityFilter,
            EMPTY_FILTER,
            relationshipTypes,
            relationships,
            expectedUrnStrings.toArray(new String[0])
    );
  }

  @DataProvider(name="FindRelatedUrnsDestinationEntityFilterTests")
  public Object[][] getFindRelatedUrnsDestinationEntityFilterTests() {
    return new Object[][] {
            new Object[] {
                    newFilter("urn", datasetTwoUrnString),
                    Arrays.asList(downstreamOf),
                    outgoingRelationships,
                    Arrays.asList(datasetTwoUrnString)
            },
            new Object[] {
                    newFilter("urn", datasetTwoUrnString),
                    Arrays.asList(downstreamOf),
                    incomingRelationships,
                    Arrays.asList(datasetTwoUrnString)
            },
            new Object[] {
                    newFilter("urn", datasetTwoUrnString),
                    Arrays.asList(downstreamOf),
                    undirectedRelationships,
                    Arrays.asList(datasetTwoUrnString)
            },

            new Object[] {
                    newFilter("urn", userOneUrnString),
                    Arrays.asList(downstreamOf),
                    outgoingRelationships,
                    Arrays.asList()
            },
            new Object[] {
                    newFilter("urn", userOneUrnString),
                    Arrays.asList(downstreamOf),
                    incomingRelationships,
                    Arrays.asList()
            },
            new Object[] {
                    newFilter("urn", userOneUrnString),
                    Arrays.asList(downstreamOf),
                    undirectedRelationships,
                    Arrays.asList()
            },

            new Object[] {
                    newFilter("urn", userOneUrnString),
                    Arrays.asList(hasOwner),
                    outgoingRelationships,
                    Arrays.asList(userOneUrnString)
            },
            new Object[] {
                    newFilter("urn", userOneUrnString),
                    Arrays.asList(hasOwner),
                    incomingRelationships,
                    Arrays.asList()
            },
            new Object[] {
                    newFilter("urn", userOneUrnString),
                    Arrays.asList(hasOwner),
                    undirectedRelationships,
                    Arrays.asList(userOneUrnString)
            }
    };
  }

  @Test(dataProvider="FindRelatedUrnsDestinationEntityFilterTests")
  public void testFindRelatedUrnsDestinationEntityFilter(Filter destinationEntityFilter,
                                                         List<String> relationshipTypes,
                                                         RelationshipFilter relationships,
                                                         List<String> expectedUrnStrings) throws Exception {
    doTestFindRelatedUrns(
            EMPTY_FILTER,
            destinationEntityFilter,
            relationshipTypes,
            relationships,
            expectedUrnStrings.toArray(new String[0])
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

    assertEqualsAsSets(relatedUrns, Arrays.asList(expectedUrnStrings));
  }

  @DataProvider(name="FindRelatedUrnsSourceTypeTests")
  public Object[][] getFindRelatedUrnsSourceTypeTests() {
    return new Object[][]{
            new Object[]{
                    datasetType,
                    Arrays.asList(downstreamOf),
                    outgoingRelationships,
                    Arrays.asList(datasetOneUrnString, datasetTwoUrnString)
            },
            new Object[]{
                    datasetType,
                    Arrays.asList(downstreamOf),
                    incomingRelationships,
                    Arrays.asList(datasetTwoUrnString, datasetThreeUrnString, datasetFourUrnString)
            },
            new Object[]{
                    datasetType,
                    Arrays.asList(downstreamOf),
                    undirectedRelationships,
                    Arrays.asList(datasetOneUrnString, datasetTwoUrnString, datasetThreeUrnString, datasetFourUrnString)
            },

            new Object[]{
                    userType,
                    Arrays.asList(downstreamOf),
                    outgoingRelationships,
                    Arrays.asList()
            },
            new Object[]{
                    userType,
                    Arrays.asList(downstreamOf),
                    incomingRelationships,
                    Arrays.asList()
            },
            new Object[]{
                    userType,
                    Arrays.asList(downstreamOf),
                    undirectedRelationships,
                    Arrays.asList()
            },

            new Object[]{
                    userType,
                    Arrays.asList(hasOwner),
                    outgoingRelationships,
                    Arrays.asList()
            },
            new Object[]{
                    userType,
                    Arrays.asList(hasOwner),
                    incomingRelationships,
                    Arrays.asList(datasetOneUrnString, datasetTwoUrnString, datasetThreeUrnString, datasetFourUrnString)
            },
            new Object[]{
                    userType,
                    Arrays.asList(hasOwner),
                    undirectedRelationships,
                    Arrays.asList(datasetOneUrnString, datasetTwoUrnString, datasetThreeUrnString, datasetFourUrnString)
            }
    };
  }

  @Test(dataProvider="FindRelatedUrnsSourceTypeTests")
  public void testFindRelatedUrnsSourceType(String datasetType,
                                            List<String> relationshipTypes,
                                            RelationshipFilter relationships,
                                            List<String> expectedUrnStrings) throws Exception {
    doTestFindRelatedUrns(
            datasetType,
            anyType,
            relationshipTypes,
            relationships,
            expectedUrnStrings.toArray(new String[0])
    );
  }

  @DataProvider(name="FindRelatedUrnsDestinationTypeTests")
  public Object[][] getFindRelatedUrnsDestinationTypeTests() {
    return new Object[][] {
            new Object[] {
                    datasetType,
                    Arrays.asList(downstreamOf),
                    outgoingRelationships,
                    Arrays.asList(datasetOneUrnString, datasetTwoUrnString)
            },
            new Object[] {
                    datasetType,
                    Arrays.asList(downstreamOf),
                    incomingRelationships,
                    Arrays.asList(datasetTwoUrnString, datasetThreeUrnString, datasetFourUrnString)
            },
            new Object[] {
                    datasetType,
                    Arrays.asList(downstreamOf),
                    undirectedRelationships,
                    Arrays.asList(datasetOneUrnString, datasetTwoUrnString, datasetThreeUrnString, datasetFourUrnString)
            },

            new Object[] {
                    datasetType,
                    Arrays.asList(hasOwner),
                    outgoingRelationships,
                    Arrays.asList()
            },
            new Object[] {
                    datasetType,
                    Arrays.asList(hasOwner),
                    incomingRelationships,
                    Arrays.asList(datasetOneUrnString, datasetTwoUrnString, datasetThreeUrnString, datasetFourUrnString)
            },
            new Object[] {
                    datasetType,
                    Arrays.asList(hasOwner),
                    undirectedRelationships,
                    Arrays.asList(datasetOneUrnString, datasetTwoUrnString, datasetThreeUrnString, datasetFourUrnString)
            },

            new Object[] {
                    userType,
                    Arrays.asList(hasOwner),
                    outgoingRelationships,
                    Arrays.asList(userOneUrnString, userTwoUrnString)
            },
            new Object[] {
                    userType,
                    Arrays.asList(hasOwner),
                    incomingRelationships,
                    Arrays.asList()
            },
            new Object[] {
                    userType,
                    Arrays.asList(hasOwner),
                    undirectedRelationships,
                    Arrays.asList(userOneUrnString, userTwoUrnString)
            }
    };
  }

  @Test(dataProvider="FindRelatedUrnsDestinationTypeTests")
  public void testFindRelatedUrnsDestinationType(String datasetType,
                                                 List<String> relationshipTypes,
                                                 RelationshipFilter relationships,
                                                 List<String> expectedUrnStrings) throws Exception {
    doTestFindRelatedUrns(
            anyType,
            datasetType,
            relationshipTypes,
            relationships,
            expectedUrnStrings.toArray(new String[0])
    );
  }

  private void doTestFindRelatedUrns(
          final String sourceType,
          final String destinationType,
          final List<String> relationshipTypes,
          final RelationshipFilter relationshipFilter,
          String... expectedUrnStrings
  ) throws Exception {
    GraphService service = getPopulatedGraphService();

    List<String> relatedUrns = service.findRelatedUrns(
            sourceType, EMPTY_FILTER,
            destinationType, EMPTY_FILTER,
            relationshipTypes, relationshipFilter,
            0, 10
    );

    assertEqualsAsSets(relatedUrns, Arrays.asList(expectedUrnStrings));
  }

  @Test
  public void testFindRelatedUrnsOffsetAndCount() throws Exception {
    GraphService service = getPopulatedGraphService();

    List<String> allRelatedUrn = service.findRelatedUrns(
            datasetType, EMPTY_FILTER,
            anyType, EMPTY_FILTER,
            Arrays.asList(downstreamOf), outgoingRelationships,
            0, 100
    );

    assertEqualsAsSets(allRelatedUrn, Arrays.asList(datasetOneUrnString, datasetTwoUrnString));

    List<String> individualRelatedUrn = new ArrayList<>();
    IntStream.range(0, allRelatedUrn.size())
            .forEach(idx -> individualRelatedUrn.addAll(
                    service.findRelatedUrns(
                    datasetType, EMPTY_FILTER,
                    anyType, EMPTY_FILTER,
                    Arrays.asList(downstreamOf), outgoingRelationships,
                    idx, 1
                )
            ));
    Assert.assertEquals(individualRelatedUrn, allRelatedUrn);
  }

  @DataProvider(name="RemoveEdgesFromNodeTests")
  public Object[][] getRemoveEdgesFromNodeTests() {
    return new Object[][] {
            new Object[] {
                    datasetTwoUrn,
                    Arrays.asList(downstreamOf),
                    outgoingRelationships,
                    Arrays.asList(datasetOneUrnString, datasetThreeUrnString, datasetFourUrnString),
                    Arrays.asList(datasetThreeUrnString, datasetFourUrnString)
            },
            new Object[] {
                    datasetTwoUrn,
                    Arrays.asList(downstreamOf),
                    incomingRelationships,
                    Arrays.asList(datasetOneUrnString, datasetThreeUrnString, datasetFourUrnString),
                    Arrays.asList(datasetOneUrnString)
            },
            new Object[] {
                    datasetTwoUrn,
                    Arrays.asList(downstreamOf),
                    undirectedRelationships,
                    Arrays.asList(datasetOneUrnString, datasetThreeUrnString, datasetFourUrnString),
                    Arrays.asList()
            }
    };
  }

  @Test(dataProvider="RemoveEdgesFromNodeTests")
  public void testRemoveEdgesFromNode(@Nonnull Urn nodeToRemoveFrom,
                                      @Nonnull List<String> relationTypes,
                                      @Nonnull RelationshipFilter relationshipFilter,
                                      List<String> expectedRelatedUrnsBeforeRemove,
                                      List<String> expectedRelatedUrnsAfterRemove) throws Exception {
    GraphService service = getPopulatedGraphService();

    List<String> allOtherRelationTypes =
            allRelationshipTypes.stream()
                    .filter(relation -> !relationTypes.contains(relation))
                    .collect(Collectors.toList());
    assertTrue(allOtherRelationTypes.size() > 0);

    List<String> actualRelatedUrnsBeforeRemove = service.findRelatedUrns(
            anyType, newFilter("urn", nodeToRemoveFrom.toString()),
            anyType, EMPTY_FILTER,
            relationTypes, undirectedRelationships,
            0, 10);
    assertEqualsAsSets(actualRelatedUrnsBeforeRemove, expectedRelatedUrnsBeforeRemove);
    List<String> relatedUrnsOfOtherRelationTypesBeforeRemove = service.findRelatedUrns(
            anyType, newFilter("urn", nodeToRemoveFrom.toString()),
            anyType, EMPTY_FILTER,
            allOtherRelationTypes, undirectedRelationships,
            0, 10);
    assertTrue(relatedUrnsOfOtherRelationTypesBeforeRemove.size() > 0);

    service.removeEdgesFromNode(
            nodeToRemoveFrom,
            relationTypes,
            relationshipFilter
    );
    syncAfterWrite();

    List<String> actualRelatedUrnsAfterRemove = service.findRelatedUrns(
            anyType, newFilter("urn", nodeToRemoveFrom.toString()),
            anyType, EMPTY_FILTER,
            relationTypes, undirectedRelationships,
            0, 10);
    assertEqualsAsSets(actualRelatedUrnsAfterRemove, expectedRelatedUrnsAfterRemove);
    List<String> relatedUrnsOfOtherRelationTypesAfterRemove = service.findRelatedUrns(
            anyType, newFilter("urn", nodeToRemoveFrom.toString()),
            anyType, EMPTY_FILTER,
            allOtherRelationTypes, undirectedRelationships,
            0, 10);
    assertEqualsAsSets(actualRelatedUrnsAfterRemove, actualRelatedUrnsBeforeRemove);
  }

  @Test
  public void testRemoveNode() throws Exception {
    GraphService service = getPopulatedGraphService();

    // assert the initial graph: check all nodes related to upstreamOf and nextVersionOf edges
    assertEqualsAsSets(
            service.findRelatedUrns(
                    datasetType, EMPTY_FILTER,
                    anyType, EMPTY_FILTER,
                    Arrays.asList(downstreamOf), undirectedRelationships,
                    0, 10
            ),
            Arrays.asList(datasetOneUrnString, datasetTwoUrnString, datasetThreeUrnString, datasetFourUrnString)
    );
    assertEqualsAsSets(
            service.findRelatedUrns(
                    userType, EMPTY_FILTER,
                    anyType, EMPTY_FILTER,
                    Arrays.asList(hasOwner), undirectedRelationships,
                    0, 10
            ),
            Arrays.asList(datasetOneUrnString, datasetTwoUrnString, datasetThreeUrnString, datasetFourUrnString)
    );

    service.removeNode(datasetTwoUrn);
    syncAfterWrite();

    // assert the modified graph: check all nodes related to upstreamOf and nextVersionOf edges again
    assertEqualsAsSets(
            service.findRelatedUrns(
                    datasetType, EMPTY_FILTER,
                    anyType, EMPTY_FILTER,
                    Arrays.asList(downstreamOf), undirectedRelationships,
                    0, 10
            ),
            Arrays.asList(datasetOneUrnString)
    );
    assertEqualsAsSets(
            service.findRelatedUrns(
                    userType, EMPTY_FILTER,
                    anyType, EMPTY_FILTER,
                    Arrays.asList(hasOwner), undirectedRelationships,
                    0, 10
            ),
            Arrays.asList(datasetOneUrnString, datasetThreeUrnString, datasetFourUrnString)
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
                    Arrays.asList(downstreamOf), undirectedRelationships,
                    0, 10
            ).toArray(),
            Arrays.asList(datasetOneUrnString, datasetTwoUrnString, datasetThreeUrnString, datasetFourUrnString).toArray()
    );
    assertEqualsNoOrder(
            service.findRelatedUrns(
                    userType, EMPTY_FILTER,
                    anyType, EMPTY_FILTER,
                    Arrays.asList(hasOwner), undirectedRelationships,
                    0, 10
            ).toArray(),
            Arrays.asList(userOneUrnString, userTwoUrnString).toArray()
    );

    service.clear();
    syncAfterWrite();

    // assert the modified graph: check all nodes related to upstreamOf and nextVersionOf edges again
    assertEqualsNoOrder(
            service.findRelatedUrns(
                    datasetType, EMPTY_FILTER,
                    anyType, EMPTY_FILTER,
                    Arrays.asList(downstreamOf), undirectedRelationships,
                    0, 10
            ).toArray(),
            new Object[0]
    );
    assertEqualsNoOrder(
            service.findRelatedUrns(
                    userType, EMPTY_FILTER,
                    anyType, EMPTY_FILTER,
                    Arrays.asList(hasOwner), undirectedRelationships,
                    0, 10
            ).toArray(),
            new Object[0]
    );
  }

}

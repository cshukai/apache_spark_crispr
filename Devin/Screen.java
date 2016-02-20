import java.io.*;
// Find approximate palindromes, of cost up to k, in a given sequence.

public class Screen
 { private static String [] shades =                      // light to dark grey
    { "#FFFFFF", "#F7F7F7", "#EEEEEE", "#DDDDDD",
      "#CCCCCC", "#BBBBBB", "#AAAAAA", "#999999" };

   public static char[] readSeq(String fileName, String dontIgnore)  // readSeq
   // A simple reader, you may want to use something else?
   // And one could consider bytes rather than chars.
    { StringBuffer sb = new StringBuffer();
      try{ File inputFile = new File( fileName );
           FileReader in  = new FileReader(inputFile);
           int c;
           while((c = in.read()) != -1)
            { if( dontIgnore.indexOf(c) < 0 ) continue;
              char ch = (char) c;
              sb.append( ch );
         }  }
      catch(Exception e)
         { System.out.println( "failed: " + e.getMessage() );
           System.exit(1);
         }//catch
      String s = sb.toString();
      char[] arr = new char[s.length()];
      for( int i = 0; i < s.length(); i++ ) arr[i] = s.charAt(i);
      return arr;
    }//readSeq

   public static void peep(char[] s, int lo, int hi, int width)
   // show a string s, just the start and end if long.
    { if( width < 5 || hi-lo+1 <= width ) width = hi-lo+1; // show all
      else // width>=5 && width < hi-lo+1
         width -= 3; // for the "..."
      for( int i = lo; i < lo+(width+1)/2; i++ ) // head
         System.out.print( s[i] );
      if( hi-lo+1 > width )  // long, must skip some
         System.out.print("..."); 
      for( int i = hi - width/2 + 1; i <= hi; i++ ) // tail
         System.out.print( s[i] );
    }//show
 
// ----------------------------------------------------------------------------
   public static int min3( int a, int b, int c)
    { return ( a < b ? ( a < c ? a : c ) : ( b < c ? b : c ) ); }//min3

   public static int max3( int a, int b, int c)
    { return ( a > b ? ( a > c ? a : c ) : ( b > c ? b : c ) ); }//max3

   public static boolean match(char c, char d) { return c == d; }//match

   public static boolean complement(char c, char d)              // complement
   // for complementary palindromes, i.e. A--T and C--G;        take your pick
    { switch(c)
       { case 'a': case 'A': return d =='t' || d == 'T';
         case 'c': case 'C': return d =='g' || d == 'G';
         case 'g': case 'G': return d =='c' || d == 'C';
         case 't': case 'T': return d =='a' || d == 'A';
         default:  return false;
    }  }//complement

// ----------------------------------------------------------------------------
                                                         // the naive algorithm
   public static void naive( char [] s )  //  [0][0] [0][1] ...
    { int len = s.length;                 //  [1][0] ...    ...
      int d[][] = new int[len][len];      //  [2][0] ...    [i][j]
      int i, j, diag;
      for( i=0; i < len; i++ )
         for( j=0; j < len; j++) d[i][j] = -1; // "clear"
      for( i=1; i < len; i++ ) d[i][i-1] = 0; // sub-diag'
      for( i=0; i < len; i++ ) d[i][i]   = 0; // main-diag,  ? 0 or 1 ???

      for( diag=1; diag < len; diag++ ) // diag, diagonal, 1,2,3,...
       { for( i=0; i < len-diag; i++ )
          { j = i+diag;  // so diag=j-i
            d[i][j] = min3( d[i][j-1]+1, d[i+1][j]+1,
                            d[i+1][j-1]+(match(s[i],s[j]) ? 0 : 1));
          }
       }

      System.out.println("<table>");
      for( i=0; i < len; i++ )  // print the matrix
       { System.out.print("<tr>");
         for( j=0; j < len; j++ )
          { int dij = d[i][j];
            String shade = shades[ dij <= 0 ? 0
                                 : dij >= shades.length ? shades.length-1
                                 : dij ];
            String sij = i == j   ? String.valueOf(s[i])
                       : dij >= 0 ? (dij <= 9 ? "_" : "")+Integer.toString(dij)
                       : " ";
            System.out.print(
               "<td" + (dij > 0 ? (" BGCOLOR=\"" + shade) : "") + "\">" +
               sij +
               "</td>" );
          }//for j
         System.out.println("</tr>\n");
       }//for i
      System.out.println("</table>\n");
    }//naive

// ----------------------------------------------------------------------------
                                            // the almost-always fast algorithm

   // Finding Approximate Palindromes in Strings Quickly and Simply
   //
   // Lloyd ALLISON,
   // School of Computer Science and Software Engineering,
   // Monash University, Clayton, Victoria, Australia 3800.
   //
   // Technical Report 2004/162, 23 Nov. 2004
   //
   // See: http://arxiv.org/pdf/cs.DS/0412004
   //
   // 10 March 2005: This code is released under the GNU GENERAL PUBLIC LICENSE
   //                http://www.gnu.org/copyleft/gpl.html

   public static void fast( char [] s, int k,
                            int maxHeapSize, boolean spaceEfficient )
  // Find approximate palindromes of cost up to `k'.
    { int len = s.length;
      int nDiags = 2*len-1;  // NB. palindrome odd, even, odd, ..., odd (!)
      int kSlices = k == 0 ? 1 : (spaceEfficient ? 2 : k+1); // two suffice
      int[][] reach = new int[nDiags][kSlices];
      int nMatches = 0;
      int heapSize = 0; // heap/priority-Q of best prospects
      Pndrm[] heap = new Pndrm[maxHeapSize];
                                                         // Boundary conditions
      for(int d=0; d < nDiags; d++ )  // NB. palindrome odd, even, odd, ... (!)
       { reach[d][0] = 0; // might change for odd palindromes?
         int i = (d+1)/2, j = d/2;  // (i,j) origin of diagonal d
         for( ; ; )                    // might have an immediate matching run?
          { i--; j++;                       // next
            if( i < 0 || j >= len ) break;  // fallen off end?
            nMatches++;                     // continues a run?
            if( match(s[i],s[j]) ) reach[d][0]++; else break;// extend or done?
       }  }

//     diag:  0 1 2 3 4  -- NB. odd diag number ~ even palindrome and v.v.!
//           / / / / /
//         ---------. 5
//         0 1 1 2 2|/   -- Numbers show distances along each diagonal.
//          / / / / | 6
//         0 0 1 1 2|/                                           .--->j
//            / / / | 7                                          |
//           0 0 1 1|/                                           |
//          /   / / | 8  -- Always an odd number of diagonals.   v
//         /   0 0 1|/                                           i
//        /       / |
//       /       0 0|     (Although fast does not actually use a d[][] matrix.)
// eg. d3
                                                                // general step
      int thisDiffs = 0;
      for(int diffs=1; diffs <= k; diffs++ )       // for 1, 2, ... differences
       { int lastDiffs = thisDiffs;
         thisDiffs = (thisDiffs+1) % kSlices;  // ~ space efficiency
         for(int d=diffs; d < nDiags-diffs; d++ )  // diagonal number d
          { int oddD = d & 1;  // NB. odd diagonal number = even palindrome!
            int rch = max3(reach[d-1][lastDiffs] + oddD,
                           reach[d  ][lastDiffs] + 1,
                           reach[d+1][lastDiffs] + oddD);                // hop
            if( rch > (d+1)/2 ) rch = (d+1)/2;  // stay in box
            if( rch > (nDiags-d)/2 ) rch = (nDiags-d)/2;
            reach[d][thisDiffs] = rch;
            int i = left(d, rch), j = right(d, rch, len);  // interval [i..j]
            for( ; ; )
             { i--; j++;                       // next
               if( i < 0 || j >= len ) break;  // fallen off end?
               nMatches++;                     // continues a run?
               if( match(s[i],s[j]) ) reach[d][thisDiffs]++;
               else break; // extend?
             }//run
          }//for d the diagonal number
       }//for diffs

      for(int d=k; d < nDiags-k; d++ )              // choose best prospects...
       { int ke = k % kSlices;  // effective k ~ space efficiency
         int i  = left(d, reach[d][ke]), j = right(d, reach[d][ke], len);
         int i1 = d > k          ?  left(d-1, reach[d-1][ke])      : i;
         int j1 = d > k          ? right(d-1, reach[d-1][ke], len) : j;
         int i2 = d < nDiags-k-1 ?  left(d+1, reach[d+1][ke])      : i;
         int j2 = d < nDiags-k-1 ? right(d+1, reach[d+1][ke], len) : j;
         if((i >= i1 && j < j1) || (i > i1 && j <= j1) ||         // ?neighbour
            (i >= i2 && j < j2) || (i > i2 && j <= j2)) continue; // contains?
         Pndrm p = new Pndrm(d, reach[d][ke]);
         if( heapSize < maxHeapSize ) // not yet full...
          { heapSize++; heap[maxHeapSize-heapSize] = p;
            downHeap(heap, maxHeapSize-heapSize, maxHeapSize-1);
          }
         else if(p.beats(heap[0])) // full, but smallest (1st) is beaten...
          { heap[0] = p; downHeap(heap, 0, maxHeapSize-1); }
       }//for
      if( heapSize < maxHeapSize ) // then shift left to a[0..]
         for(int i=0; i < heapSize; i++) heap[i]=heap[i+maxHeapSize-heapSize];
      for( int i = heapSize - 1; i > 0; i-- ) // now sort and print prospects
       { Pndrm tmp = heap[i]; heap[i] = heap[0]; heap[0] = tmp; // i.e. swap
         downHeap(heap, 0, i-1);               // and restore (smaller) heap
       }
                                                                    // printing
      System.out.println("<h4>fast(s[" + String.valueOf(len)
                +  "], k="            + String.valueOf(k)
                + ", maxHeapSize="    + String.valueOf(maxHeapSize)
                + ", spaceEfficient=" + String.valueOf(spaceEfficient)
                + ")</h4>\n"); // heading

      System.out.println( String.valueOf(nMatches) + " comparisons = "
       + String.valueOf(Math.round(10.0*nMatches/(len*(k+1)))/10.0)
       + " * (k+1) * n <br>\n" );  // statistics

      System.out.println( "<pre>\n" );   // candidate palindromes...
      for( int i=0; i < heapSize; i++ )
       { System.out.print("palindrome " + String.valueOf(i+1) + " = "
            + "s[" + String.valueOf(left(heap[i].d,  heap[i].r))
            + ".." + String.valueOf(right(heap[i].d, heap[i].r, len))
            +  "], diag " + String.valueOf(heap[i].d)
            + ", reach " + String.valueOf(heap[i].r) + ", " );
         peep(s, left(heap[i].d,  heap[i].r),
                 right(heap[i].d, heap[i].r, len), 20); // to help locate it
         System.out.println();
       }
      System.out.println( "</pre>\n" );

      if(len > 130) return;

                                      // tabular form, only for short sequences
      System.out.println("<table BORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"1\" >");
      for(int diffs=k; diffs > k-kSlices; diffs-- )
       { int big = -1;
         for(int d=diffs; d < nDiags-diffs; d++ ) // find biggest reach in row
          { int i = (d+1)/2 - reach[d][diffs%kSlices],
                j = d/2     + reach[d][diffs%kSlices];
            if( i >= 0 && j < len && reach[d][diffs%kSlices] > big )
               big = reach[d][diffs%kSlices];
          }
         if( big < 0 ) continue; // all diagonals off the end for diffs

         System.out.print( "<tr><td>" + String.valueOf(diffs) + ":</td>");
         for( int dummy=0; dummy < diffs; dummy++ )
            System.out.print( "<td> </td>" );
         for(int d=diffs; d < nDiags-diffs; d++ )
          { int i = (d+1)/2 - reach[d][diffs%kSlices],
                j = d/2 + reach[d][diffs%kSlices];
            String sdd = "e";  // i.e. end of diagonal (paranoid programming)
            int under = 0;
            if( i >= 0 && j < len ) // i.e. proper region
             { sdd = String.valueOf(reach[d][diffs%kSlices]);
               under = big - reach[d][diffs%kSlices];
               under = under < 0 ? 0
                      : under >= shades.length ? shades.length - 1
                      : under;
             }
            System.out.print("<td BGCOLOR=\"" + shades[under] + "\">");
            System.out.print( sdd );
            System.out.print("</td>");
          }
         System.out.println("</tr>\n");
       }//for diffs
      System.out.println("<tr><td> d/10: </td>");
      for(int d=0; d < nDiags; d++ ) // put on some "coordinates"
         System.out.print("<td>"+(d%10==0?String.valueOf(d/10):"_")+"</td>");
      System.out.println("</tr>");
      System.out.println("</table>\n"); // end of tabular form

    }//fast(...)

// ----------------------------------------------------------------------------
   public static int left(int diag, int reach)                     // auxiliary
    { int l = (diag+1)/2 - reach; return l >= 0 ? l : 0; }

   public static int right(int diag, int reach, int len)
    { int r = diag/2 + reach; return r < len ? r : len-1; }

   public static class Pndrm            // a pair:  < diagonal#, reach >
    { public int d, r;
      public Pndrm(int diagonal, int reach) { d = diagonal; r = reach; }
      public boolean beats (Pndrm p2)   // i.e. what do you "prefer" ?
       { return this.r > p2.r || (this.r == p2.r && this.d < p2.d); }
    }//class Pndrm

   private static void downHeap(Pndrm[] h, int lo, int hi) //      0
   // PRE:  h[lo+1..hi] is a heap.                               1   2
   // POST: h[lo..hi] is a heap (with same contents).           3 4 5 6
    { int parent = lo, child = (lo+1)*2-1;           // NB. smallest elt on top
      Pndrm newElt = h[parent];
      while( child <= hi )
       { if( child < hi && h[child].beats(h[child+1]) ) child++; // i.e. right
         if( h[child].beats(newElt) ) break;
         h[parent] = h[child];  // move child up
         parent = child;  child = (parent+1)*2-1; // i.e. left child
       }                   // Now newElt less than both children, if any, so...
      h[parent] = newElt;
    }//downHeap

// ----------------------------------------------------------------------------
   public static void  main(String[] argv)                            // main()
   { System.out.println("#--- Screen.java, L.A., 27/7/2004, CSSE, Monash, .au ---");
     int k = 3; // default

     for(int i=0; i < argv.length; i++)           // command line params if any
        System.out.println("# argv[" + i + "] = " + argv[i]);

     String select   = "";
     double fraction = 0.5;
     String fileName = "";
     try { if(argv.length > 1)
              k = new Integer(argv[0]).intValue();
           fileName = argv[argv.length-1];      // last param is the filename
         }
     catch(Exception e)
      { System.out.println("usage: java prog_name [k] filename\n"
           + "default k = 3;  runs through complexity tests if k < 0");
        System.exit(1);
      }//catch

     char[] arr = readSeq( fileName, "acgtuACGTUrnyRNY" );  // INPUT THE STRING
     // NB. you may want to change the set of characters that are not ignored.

     System.out.print("# " + arr.length + "bp, ");
     peep(arr, 0, arr.length-1, 60); System.out.println();
     // for(int i=0; i < (arr.length < 65 ? arr.length : 65); i++) // show a bit
        // System.out.print( arr[i] );
     // System.out.println( arr.length > 65 ? "..." : "" );

     if( arr.length <= 50)                // O(n**2) space and time !
        naive( arr );                     // 1. the "obvious" algorithm

     if( k >= 0 )
        fast( arr, k, 10, true );        // 2. faster, O(k*n)
     else // k < 0, run through some time-complexity tests
      { fast( arr, 0, 3, false );                           // NB. here space
        for( k=1; k <= 16; k*=2 ) fast( arr, k, 3, false ); //  in-efficient.
      }

     System.out.println("#--- end ---");
   }//main

 }//Screen class
// L.Allison, School of Comp. Sci. and SWE (CSSE), Monash U., Australia 3800.
// ----------------------------------------------------------------------------
